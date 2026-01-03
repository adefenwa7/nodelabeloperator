/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodelabelv1alpha1 "github.com/adefenwa7/node-labeler-operator/api/v1alpha1"
)

const (
	FinalizerName      = "nodelabel.nodelabel.io/finalizer"
	ConditionTypeReady = "Ready"
	ConditionTypeError = "Error"
	RequeueInterval    = 5 * time.Minute

	// Resource names
	ResourceNvidiaGPU      = "nvidia.com/gpu"
	ResourceAMDGPU         = "amd.com/gpu"
	ResourceHugepages2Mi   = "hugepages-2Mi"
	ResourceHugepages1Gi   = "hugepages-1Gi"
	ResourceEphemeralStore = "ephemeral-storage"
	ResourcePods           = "pods"

	// Well-known node labels for topology
	LabelArch         = "kubernetes.io/arch"
	LabelOS           = "kubernetes.io/os"
	LabelZone         = "topology.kubernetes.io/zone"
	LabelRegion       = "topology.kubernetes.io/region"
	LabelInstanceType = "node.kubernetes.io/instance-type"
)

// Event reasons
const (
	EventReasonLabelsApplied      = "LabelsApplied"
	EventReasonLabelsRemoved      = "LabelsRemoved"
	EventReasonStaleLabelsRemoved = "StaleLabelsRemoved"
	EventReasonSelectorChanged    = "SelectorChanged"
	EventReasonFinalizerAdded     = "FinalizerAdded"
	EventReasonFinalizerRemoved   = "FinalizerRemoved"
	EventReasonReconcileSuccess   = "ReconcileSuccess"
	EventReasonReconcileFailed    = "ReconcileFailed"
	EventReasonDeletionCleanup    = "DeletionCleanup"
	EventReasonNoMatchingNodes    = "NoMatchingNodes"
	EventReasonGPUDetected        = "GPUDetected"
	EventReasonSRIOVDetected      = "SRIOVDetected"
	EventReasonAutoDetected       = "AutoDetected"
)

type NodeLabelReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=nodelabel.nodelabel.io,resources=nodelabels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nodelabel.nodelabel.io,resources=nodelabels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nodelabel.nodelabel.io,resources=nodelabels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *NodeLabelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Starting reconciliation", "name", req.Name)

	nodeLabel := &nodelabelv1alpha1.NodeLabel{}
	if err := r.Get(ctx, req.NamespacedName, nodeLabel); err != nil {
		if errors.IsNotFound(err) {
			log.Info("NodeLabel resource not found - likely deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to fetch NodeLabel")
		return ctrl.Result{}, err
	}

	if !nodeLabel.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, nodeLabel)
	}

	if result, requeue := r.manageFinalizer(ctx, nodeLabel); requeue {
		return result, nil
	}

	labelsChanged := !reflect.DeepEqual(nodeLabel.Spec.Labels, nodeLabel.Status.AppliedLabels)
	selectorChanged := !reflect.DeepEqual(nodeLabel.Spec.NodeSelector, nodeLabel.Status.AppliedNodeSelector)

	if selectorChanged && len(nodeLabel.Status.LabeledNodes) > 0 {
		log.Info("Node selector changed, cleaning up previously labeled nodes")
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonSelectorChanged,
			"Node selector changed, cleaning up previously labeled nodes")
		if err := r.cleanupPreviouslyLabeledNodes(ctx, nodeLabel); err != nil {
			r.setErrorCondition(ctx, nodeLabel, "CleanupFailed", err.Error())
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}

	if labelsChanged && len(nodeLabel.Status.AppliedLabels) > 0 {
		staleLabels := r.findStaleLabels(nodeLabel)
		if len(staleLabels) > 0 {
			r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonStaleLabelsRemoved,
				fmt.Sprintf("Removing stale labels: %s", formatLabels(staleLabels)))
			if err := r.removeStaleLabels(ctx, nodeLabel, staleLabels); err != nil {
				r.setErrorCondition(ctx, nodeLabel, "StaleLabelsRemovalFailed", err.Error())
				return ctrl.Result{RequeueAfter: 30 * time.Second}, err
			}
		}
	}

	matchingNodes, err := r.getMatchingNodes(ctx, nodeLabel.Spec.NodeSelector)
	if err != nil {
		r.recordEvent(nodeLabel, corev1.EventTypeWarning, EventReasonReconcileFailed,
			fmt.Sprintf("Failed to list matching nodes: %v", err))
		r.setErrorCondition(ctx, nodeLabel, "FailedToListNodes", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	log.Info("Found matching nodes", "count", len(matchingNodes))

	if len(matchingNodes) == 0 {
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonNoMatchingNodes,
			"No nodes match the specified selector")
	}

	labeledNodes, nodesUpdated, stats, autoDetectedLabels, err := r.applyLabelsToNodes(ctx, matchingNodes, nodeLabel)
	if err != nil {
		r.recordEvent(nodeLabel, corev1.EventTypeWarning, EventReasonReconcileFailed,
			fmt.Sprintf("Failed to apply labels to nodes: %v", err))
		r.setErrorCondition(ctx, nodeLabel, "FailedToApplyLabels", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	if nodesUpdated > 0 && len(nodeLabel.Spec.Labels) > 0 {
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonLabelsApplied,
			fmt.Sprintf("Applied labels to %d node(s): %s", nodesUpdated, formatLabels(nodeLabel.Spec.Labels)))
	}

	if stats.gpuNodes > 0 {
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonGPUDetected,
			fmt.Sprintf("Detected GPU on %d node(s)", stats.gpuNodes))
	}

	if stats.sriovNodes > 0 {
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonSRIOVDetected,
			fmt.Sprintf("Detected SR-IOV on %d node(s)", stats.sriovNodes))
	}

	if err := r.updateStatus(ctx, nodeLabel, matchingNodes, labeledNodes, stats, autoDetectedLabels); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonReconcileSuccess,
		fmt.Sprintf("Successfully reconciled: %d/%d nodes labeled, %d GPU, %d SR-IOV",
			len(labeledNodes), len(matchingNodes), stats.gpuNodes, stats.sriovNodes))

	return ctrl.Result{RequeueAfter: RequeueInterval}, nil
}

// detectionStats holds counts of detected node types
type detectionStats struct {
	gpuNodes   int
	sriovNodes int
}

func (r *NodeLabelReconciler) applyLabelsToNodes(ctx context.Context, nodes []corev1.Node, nodeLabel *nodelabelv1alpha1.NodeLabel) ([]string, int, detectionStats, map[string]map[string]string, error) {
	log := logf.FromContext(ctx)
	var labeledNodes []string
	nodesUpdated := 0
	stats := detectionStats{}
	autoDetectedLabels := make(map[string]map[string]string)

	for _, node := range nodes {
		nodeCopy := node.DeepCopy()
		needsUpdate := false

		if nodeCopy.Labels == nil {
			nodeCopy.Labels = make(map[string]string)
		}

		// Apply static labels
		for key, value := range nodeLabel.Spec.Labels {
			if existing, ok := nodeCopy.Labels[key]; !ok || existing != value {
				nodeCopy.Labels[key] = value
				needsUpdate = true
			}
		}

		// Apply auto-detected labels
		detectedLabels := r.detectNodeCapabilities(nodeCopy, nodeLabel.Spec.AutoDetect)
		autoDetectedLabels[node.Name] = detectedLabels

		for key, value := range detectedLabels {
			if existing, ok := nodeCopy.Labels[key]; !ok || existing != value {
				nodeCopy.Labels[key] = value
				needsUpdate = true
			}
		}

		// Update stats
		if r.hasGPU(nodeCopy) {
			stats.gpuNodes++
		}
		if r.hasSRIOV(nodeCopy, nodeLabel.Spec.AutoDetect) {
			stats.sriovNodes++
		}

		if needsUpdate {
			log.Info("Applying labels to node", "node", node.Name,
				"staticLabels", nodeLabel.Spec.Labels,
				"autoDetectedLabels", detectedLabels)
			if err := r.Update(ctx, nodeCopy); err != nil {
				if errors.IsConflict(err) {
					log.Info("Conflict updating node, will retry", "node", node.Name)
					continue
				}
				return labeledNodes, nodesUpdated, stats, autoDetectedLabels,
					fmt.Errorf("failed to update node %s: %w", node.Name, err)
			}
			nodesUpdated++
		}

		labeledNodes = append(labeledNodes, node.Name)
	}

	return labeledNodes, nodesUpdated, stats, autoDetectedLabels, nil
}

func (r *NodeLabelReconciler) detectNodeCapabilities(node *corev1.Node, rules []nodelabelv1alpha1.AutoDetectRule) map[string]string {
	detectedLabels := make(map[string]string)

	for _, rule := range rules {
		detected, labelKey, labelValue := r.evaluateAutoDetectRule(node, rule)
		if detected && labelKey != "" {
			detectedLabels[labelKey] = labelValue
		}
	}

	return detectedLabels
}

func (r *NodeLabelReconciler) evaluateAutoDetectRule(node *corev1.Node, rule nodelabelv1alpha1.AutoDetectRule) (bool, string, string) {
	switch rule.Type {
	// GPU Detection
	case nodelabelv1alpha1.AutoDetectGPU:
		return r.detectAnyGPU(node, rule)
	case nodelabelv1alpha1.AutoDetectNvidiaGPU:
		return r.detectResource(node, rule, ResourceNvidiaGPU, "nvidia-gpu", "present", resource.MustParse("1"))
	case nodelabelv1alpha1.AutoDetectAMDGPU:
		return r.detectResource(node, rule, ResourceAMDGPU, "amd-gpu", "present", resource.MustParse("1"))

	// Resource Detection
	case nodelabelv1alpha1.AutoDetectMemory:
		return r.detectResource(node, rule, "memory", "high-memory", "true", resource.MustParse("64Gi"))
	case nodelabelv1alpha1.AutoDetectCPU:
		return r.detectResource(node, rule, "cpu", "high-cpu", "true", resource.MustParse("16"))
	case nodelabelv1alpha1.AutoDetectEphemeralStorage:
		return r.detectResource(node, rule, ResourceEphemeralStore, "high-ephemeral-storage", "true", resource.MustParse("100Gi"))
	case nodelabelv1alpha1.AutoDetectPods:
		return r.detectResource(node, rule, ResourcePods, "high-pod-capacity", "true", resource.MustParse("100"))

	// Hugepages Detection
	case nodelabelv1alpha1.AutoDetectHugepages2Mi:
		return r.detectResource(node, rule, ResourceHugepages2Mi, "hugepages-2Mi", "present", resource.MustParse("1"))
	case nodelabelv1alpha1.AutoDetectHugepages1Gi:
		return r.detectResource(node, rule, ResourceHugepages1Gi, "hugepages-1Gi", "present", resource.MustParse("1"))

	// Topology Detection (from node labels)
	case nodelabelv1alpha1.AutoDetectArch:
		return r.detectFromNodeLabel(node, rule, LabelArch, "arch")
	case nodelabelv1alpha1.AutoDetectOS:
		return r.detectFromNodeLabel(node, rule, LabelOS, "os")
	case nodelabelv1alpha1.AutoDetectZone:
		return r.detectFromNodeLabel(node, rule, LabelZone, "zone")
	case nodelabelv1alpha1.AutoDetectRegion:
		return r.detectFromNodeLabel(node, rule, LabelRegion, "region")
	case nodelabelv1alpha1.AutoDetectInstanceType:
		return r.detectFromNodeLabel(node, rule, LabelInstanceType, "instance-type")

	// Network Detection
	case nodelabelv1alpha1.AutoDetectSRIOV:
		return r.detectSRIOV(node, rule)

	// Custom Resource Detection
	case nodelabelv1alpha1.AutoDetectCustom:
		if rule.ResourceName == "" {
			return false, "", ""
		}
		defaultKey := strings.ReplaceAll(rule.ResourceName, "/", "-")
		defaultKey = strings.ReplaceAll(defaultKey, ".", "-")
		return r.detectResource(node, rule, rule.ResourceName, defaultKey, "present", resource.MustParse("1"))

	default:
		return false, "", ""
	}
}

// detectAnyGPU detects any GPU (NVIDIA or AMD)
func (r *NodeLabelReconciler) detectAnyGPU(node *corev1.Node, rule nodelabelv1alpha1.AutoDetectRule) (bool, string, string) {
	nvidiaQty := r.getNodeCapacity(node, ResourceNvidiaGPU)
	amdQty := r.getNodeCapacity(node, ResourceAMDGPU)
	totalGPU := nvidiaQty.Value() + amdQty.Value()

	if totalGPU > 0 {
		labelKey := rule.LabelKey
		if labelKey == "" {
			labelKey = "gpu"
		}
		labelValue := rule.LabelValue
		if labelValue == "" {
			labelValue = "present"
		}
		if rule.LabelValueFromQuantity {
			labelValue = strconv.FormatInt(totalGPU, 10)
		}
		return true, labelKey, labelValue
	}
	return false, "", ""
}

// detectResource detects a resource quantity from node capacity
func (r *NodeLabelReconciler) detectResource(node *corev1.Node, rule nodelabelv1alpha1.AutoDetectRule,
	resourceName, defaultLabelKey, defaultLabelValue string, defaultMinQty resource.Quantity) (bool, string, string) {

	qty := r.getNodeCapacity(node, resourceName)

	// If OnlyIfPresent is set and resource is zero, skip
	if rule.OnlyIfPresent && qty.IsZero() {
		return false, "", ""
	}

	minQty := defaultMinQty
	if rule.MinQuantity != nil {
		minQty = *rule.MinQuantity
	}

	if qty.Cmp(minQty) >= 0 {
		labelKey := rule.LabelKey
		if labelKey == "" {
			labelKey = defaultLabelKey
		}
		labelValue := rule.LabelValue
		if labelValue == "" {
			labelValue = defaultLabelValue
		}
		if rule.LabelValueFromQuantity {
			// Format quantity appropriately
			if resourceName == "memory" || resourceName == ResourceEphemeralStore {
				// Convert to Gi for readability
				valueInGi := qty.Value() / (1024 * 1024 * 1024)
				labelValue = fmt.Sprintf("%dGi", valueInGi)
			} else {
				labelValue = qty.String()
			}
		}
		return true, labelKey, labelValue
	}

	return false, "", ""
}

// detectFromNodeLabel detects by copying or checking existing node labels
func (r *NodeLabelReconciler) detectFromNodeLabel(node *corev1.Node, rule nodelabelv1alpha1.AutoDetectRule,
	defaultSourceLabel, defaultLabelKey string) (bool, string, string) {

	sourceLabel := defaultSourceLabel
	if rule.SourceNodeLabel != "" {
		sourceLabel = rule.SourceNodeLabel
	}

	value, exists := node.Labels[sourceLabel]
	if !exists || value == "" {
		if rule.OnlyIfPresent {
			return false, "", ""
		}
		return false, "", ""
	}

	labelKey := rule.LabelKey
	if labelKey == "" {
		labelKey = defaultLabelKey
	}

	labelValue := value
	if rule.LabelValue != "" {
		labelValue = rule.LabelValue
	}
	if rule.LabelValueFromNodeLabel != "" {
		if v, ok := node.Labels[rule.LabelValueFromNodeLabel]; ok {
			labelValue = v
		}
	}

	return true, labelKey, labelValue
}

// detectSRIOV detects SR-IOV network devices
func (r *NodeLabelReconciler) detectSRIOV(node *corev1.Node, rule nodelabelv1alpha1.AutoDetectRule) (bool, string, string) {
	// Check for common SR-IOV resource patterns
	sriovResources := []string{
		"intel.com/sriov_netdevice",
		"intel.com/sriov_net",
		"mellanox.com/sriov",
		"nvidia.com/sriov",
	}

	// If user specified a custom resource name, use that
	if rule.ResourceName != "" {
		sriovResources = []string{rule.ResourceName}
	}

	totalSRIOV := int64(0)
	for _, resName := range sriovResources {
		qty := r.getNodeCapacity(node, resName)
		totalSRIOV += qty.Value()
	}

	if totalSRIOV > 0 {
		labelKey := rule.LabelKey
		if labelKey == "" {
			labelKey = "sriov"
		}
		labelValue := rule.LabelValue
		if labelValue == "" {
			labelValue = "present"
		}
		if rule.LabelValueFromQuantity {
			labelValue = strconv.FormatInt(totalSRIOV, 10)
		}
		return true, labelKey, labelValue
	}

	return false, "", ""
}

func (r *NodeLabelReconciler) getNodeCapacity(node *corev1.Node, resourceName string) resource.Quantity {
	resName := corev1.ResourceName(resourceName)

	if qty, ok := node.Status.Capacity[resName]; ok {
		return qty
	}
	if qty, ok := node.Status.Allocatable[resName]; ok {
		return qty
	}
	return resource.Quantity{}
}

func (r *NodeLabelReconciler) hasGPU(node *corev1.Node) bool {
	nvidiaQty := r.getNodeCapacity(node, ResourceNvidiaGPU)
	amdQty := r.getNodeCapacity(node, ResourceAMDGPU)
	return nvidiaQty.Value() > 0 || amdQty.Value() > 0
}

func (r *NodeLabelReconciler) hasSRIOV(node *corev1.Node, rules []nodelabelv1alpha1.AutoDetectRule) bool {
	for _, rule := range rules {
		if rule.Type == nodelabelv1alpha1.AutoDetectSRIOV {
			detected, _, _ := r.detectSRIOV(node, rule)
			return detected
		}
	}
	return false
}

func (r *NodeLabelReconciler) recordEvent(nodeLabel *nodelabelv1alpha1.NodeLabel, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(nodeLabel, eventType, reason, message)
	}
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, ", ")
}

func (r *NodeLabelReconciler) findStaleLabels(nodeLabel *nodelabelv1alpha1.NodeLabel) map[string]string {
	staleLabels := make(map[string]string)
	for key, value := range nodeLabel.Status.AppliedLabels {
		if _, exists := nodeLabel.Spec.Labels[key]; !exists {
			staleLabels[key] = value
		}
	}
	return staleLabels
}

func (r *NodeLabelReconciler) manageFinalizer(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel) (ctrl.Result, bool) {
	log := logf.FromContext(ctx)

	if nodeLabel.Spec.RemoveOnDelete {
		if !controllerutil.ContainsFinalizer(nodeLabel, FinalizerName) {
			log.Info("Adding finalizer")
			controllerutil.AddFinalizer(nodeLabel, FinalizerName)
			if err := r.Update(ctx, nodeLabel); err != nil {
				return ctrl.Result{}, true
			}
			r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonFinalizerAdded,
				"Finalizer added - labels will be removed when resource is deleted")
			return ctrl.Result{Requeue: true}, true
		}
	} else {
		if controllerutil.ContainsFinalizer(nodeLabel, FinalizerName) {
			log.Info("Removing finalizer (removeOnDelete is false)")
			controllerutil.RemoveFinalizer(nodeLabel, FinalizerName)
			if err := r.Update(ctx, nodeLabel); err != nil {
				return ctrl.Result{}, true
			}
			r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonFinalizerRemoved,
				"Finalizer removed")
			return ctrl.Result{Requeue: true}, true
		}
	}
	return ctrl.Result{}, false
}

func (r *NodeLabelReconciler) handleDeletion(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if controllerutil.ContainsFinalizer(nodeLabel, FinalizerName) {
		log.Info("Handling deletion - removing labels from nodes")
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonDeletionCleanup,
			"Resource being deleted, removing labels from nodes")

		allLabelsToRemove := make(map[string]string)
		for k, v := range nodeLabel.Status.AppliedLabels {
			allLabelsToRemove[k] = v
		}
		for _, nodeLabels := range nodeLabel.Status.AutoDetectedLabels {
			for k, v := range nodeLabels {
				allLabelsToRemove[k] = v
			}
		}

		if len(nodeLabel.Status.LabeledNodes) > 0 && len(allLabelsToRemove) > 0 {
			removedCount, err := r.removeLabelsFromNodesByName(ctx, nodeLabel.Status.LabeledNodes, allLabelsToRemove)
			if err != nil {
				r.recordEvent(nodeLabel, corev1.EventTypeWarning, EventReasonReconcileFailed,
					fmt.Sprintf("Failed to remove labels during deletion: %v", err))
				return ctrl.Result{}, err
			}
			r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonLabelsRemoved,
				fmt.Sprintf("Removed labels from %d node(s) during deletion cleanup", removedCount))
		}

		controllerutil.RemoveFinalizer(nodeLabel, FinalizerName)
		if err := r.Update(ctx, nodeLabel); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *NodeLabelReconciler) cleanupPreviouslyLabeledNodes(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel) error {
	log := logf.FromContext(ctx)

	newMatchingNodes, err := r.getMatchingNodes(ctx, nodeLabel.Spec.NodeSelector)
	if err != nil {
		return fmt.Errorf("failed to get new matching nodes: %w", err)
	}

	newMatchingSet := make(map[string]bool)
	for _, node := range newMatchingNodes {
		newMatchingSet[node.Name] = true
	}

	var nodesToClean []string
	for _, nodeName := range nodeLabel.Status.LabeledNodes {
		if !newMatchingSet[nodeName] {
			nodesToClean = append(nodesToClean, nodeName)
		}
	}

	if len(nodesToClean) > 0 {
		log.Info("Cleaning up nodes that no longer match selector", "nodes", nodesToClean)

		labelsToRemove := make(map[string]string)
		for k, v := range nodeLabel.Status.AppliedLabels {
			labelsToRemove[k] = v
		}
		for _, nodeName := range nodesToClean {
			if nodeLabels, ok := nodeLabel.Status.AutoDetectedLabels[nodeName]; ok {
				for k, v := range nodeLabels {
					labelsToRemove[k] = v
				}
			}
		}

		removedCount, err := r.removeLabelsFromNodesByName(ctx, nodesToClean, labelsToRemove)
		if err != nil {
			return err
		}
		r.recordEvent(nodeLabel, corev1.EventTypeNormal, EventReasonLabelsRemoved,
			fmt.Sprintf("Removed labels from %d node(s) that no longer match selector", removedCount))
	}

	return nil
}

func (r *NodeLabelReconciler) removeStaleLabels(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel, staleLabels map[string]string) error {
	if len(staleLabels) == 0 {
		return nil
	}
	_, err := r.removeLabelsFromNodesByName(ctx, nodeLabel.Status.LabeledNodes, staleLabels)
	return err
}

func (r *NodeLabelReconciler) getMatchingNodes(ctx context.Context, selector map[string]string) ([]corev1.Node, error) {
	nodeList := &corev1.NodeList{}
	listOpts := []client.ListOption{}
	if len(selector) > 0 {
		listOpts = append(listOpts, client.MatchingLabels(selector))
	}
	if err := r.List(ctx, nodeList, listOpts...); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

func (r *NodeLabelReconciler) removeLabelsFromNodesByName(ctx context.Context, nodeNames []string, labelsToRemove map[string]string) (int, error) {
	log := logf.FromContext(ctx)
	nodesUpdated := 0

	for _, nodeName := range nodeNames {
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return nodesUpdated, fmt.Errorf("failed to get node %s: %w", nodeName, err)
		}

		nodeCopy := node.DeepCopy()
		needsUpdate := false

		for key := range labelsToRemove {
			if _, ok := nodeCopy.Labels[key]; ok {
				delete(nodeCopy.Labels, key)
				needsUpdate = true
			}
		}

		if needsUpdate {
			log.Info("Removing labels from node", "node", nodeName)
			if err := r.Update(ctx, nodeCopy); err != nil {
				if errors.IsConflict(err) {
					continue
				}
				return nodesUpdated, fmt.Errorf("failed to update node %s: %w", nodeName, err)
			}
			nodesUpdated++
		}
	}

	return nodesUpdated, nil
}

func (r *NodeLabelReconciler) updateStatus(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel,
	matchingNodes []corev1.Node, labeledNodes []string, stats detectionStats, autoDetectedLabels map[string]map[string]string) error {

	now := metav1.Now()

	nodeLabel.Status.AppliedLabels = make(map[string]string)
	for k, v := range nodeLabel.Spec.Labels {
		nodeLabel.Status.AppliedLabels[k] = v
	}

	nodeLabel.Status.AppliedNodeSelector = make(map[string]string)
	for k, v := range nodeLabel.Spec.NodeSelector {
		nodeLabel.Status.AppliedNodeSelector[k] = v
	}

	nodeLabel.Status.AutoDetectedLabels = autoDetectedLabels
	nodeLabel.Status.MatchingNodeCount = len(matchingNodes)
	nodeLabel.Status.LabeledNodeCount = len(labeledNodes)
	nodeLabel.Status.LabeledNodes = labeledNodes
	nodeLabel.Status.GPUNodeCount = stats.gpuNodes
	nodeLabel.Status.SRIOVNodeCount = stats.sriovNodes
	nodeLabel.Status.LastUpdated = &now

	meta.SetStatusCondition(&nodeLabel.Status.Conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "ReconcileSuccess",
		Message:            fmt.Sprintf("Labeled %d/%d nodes, %d GPU, %d SR-IOV", len(labeledNodes), len(matchingNodes), stats.gpuNodes, stats.sriovNodes),
		LastTransitionTime: now,
		ObservedGeneration: nodeLabel.Generation,
	})

	meta.RemoveStatusCondition(&nodeLabel.Status.Conditions, ConditionTypeError)

	return r.Status().Update(ctx, nodeLabel)
}

func (r *NodeLabelReconciler) setErrorCondition(ctx context.Context, nodeLabel *nodelabelv1alpha1.NodeLabel, reason, message string) {
	log := logf.FromContext(ctx)
	now := metav1.Now()

	meta.SetStatusCondition(&nodeLabel.Status.Conditions, metav1.Condition{
		Type:               ConditionTypeError,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: nodeLabel.Generation,
	})

	meta.SetStatusCondition(&nodeLabel.Status.Conditions, metav1.Condition{
		Type:               ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: nodeLabel.Generation,
	})

	if err := r.Status().Update(ctx, nodeLabel); err != nil {
		log.Error(err, "Failed to update error condition")
	}
}

func (r *NodeLabelReconciler) nodeMapFunc(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	nodeLabelList := &nodelabelv1alpha1.NodeLabelList{}
	if err := r.List(ctx, nodeLabelList); err != nil {
		log.Error(err, "Failed to list NodeLabels")
		return nil
	}

	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}

	var requests []reconcile.Request
	for _, nl := range nodeLabelList.Items {
		if r.nodeMatchesSelector(node, nl.Spec.NodeSelector) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKey{Name: nl.Name},
			})
		}
	}

	return requests
}

func (r *NodeLabelReconciler) nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	nodeLabels := labels.Set(node.Labels)
	selectorSet := labels.Set(selector)
	return selectorSet.AsSelector().Matches(nodeLabels)
}

func (r *NodeLabelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorderFor("nodelabel-controller")

	return ctrl.NewControllerManagedBy(mgr).
		For(&nodelabelv1alpha1.NodeLabel{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.nodeMapFunc),
		).
		Named("nodelabel").
		Complete(r)
}
