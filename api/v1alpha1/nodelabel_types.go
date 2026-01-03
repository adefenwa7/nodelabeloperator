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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AutoDetectType defines the type of auto-detection to perform
// +kubebuilder:validation:Enum=gpu;nvidia-gpu;amd-gpu;memory;cpu;ephemeral-storage;pods;hugepages-2Mi;hugepages-1Gi;arch;os;zone;region;instance-type;sriov;custom
type AutoDetectType string

const (
	// GPU Detection
	AutoDetectGPU       AutoDetectType = "gpu"
	AutoDetectNvidiaGPU AutoDetectType = "nvidia-gpu"
	AutoDetectAMDGPU    AutoDetectType = "amd-gpu"

	// Resource Detection
	AutoDetectMemory           AutoDetectType = "memory"
	AutoDetectCPU              AutoDetectType = "cpu"
	AutoDetectEphemeralStorage AutoDetectType = "ephemeral-storage"
	AutoDetectPods             AutoDetectType = "pods"

	// Hugepages Detection
	AutoDetectHugepages2Mi AutoDetectType = "hugepages-2Mi"
	AutoDetectHugepages1Gi AutoDetectType = "hugepages-1Gi"

	// Topology Detection (from node labels)
	AutoDetectArch         AutoDetectType = "arch"
	AutoDetectOS           AutoDetectType = "os"
	AutoDetectZone         AutoDetectType = "zone"
	AutoDetectRegion       AutoDetectType = "region"
	AutoDetectInstanceType AutoDetectType = "instance-type"

	// Network Detection
	AutoDetectSRIOV AutoDetectType = "sriov"

	// Custom Resource Detection
	AutoDetectCustom AutoDetectType = "custom"
)

// AutoDetectRule defines a rule for auto-detecting node capabilities
type AutoDetectRule struct {
	// Type is the type of auto-detection to perform.
	// +kubebuilder:validation:Required
	Type AutoDetectType `json:"type"`

	// ResourceName is the name of the resource to check in node capacity.
	// Required when Type is "custom" or "sriov".
	// Examples: "nvidia.com/gpu", "intel.com/sriov_netdevice", "memory"
	// +optional
	ResourceName string `json:"resourceName,omitempty"`

	// MinQuantity is the minimum quantity of the resource required.
	// Defaults vary by type (1 for GPUs, 64Gi for memory, etc.)
	// +optional
	MinQuantity *resource.Quantity `json:"minQuantity,omitempty"`

	// LabelKey is the label key to apply when the condition is met.
	// Defaults to an appropriate key based on the detection type.
	// +optional
	LabelKey string `json:"labelKey,omitempty"`

	// LabelValue is the label value to apply when the condition is met.
	// Defaults to "true" or an appropriate value based on the detection type.
	// +optional
	LabelValue string `json:"labelValue,omitempty"`

	// LabelValueFromQuantity if true, uses the detected quantity as the label value.
	// +optional
	LabelValueFromQuantity bool `json:"labelValueFromQuantity,omitempty"`

	// LabelValueFromNodeLabel if set, copies the value from the specified node label.
	// Useful for propagating existing labels like topology.kubernetes.io/zone.
	// +optional
	LabelValueFromNodeLabel string `json:"labelValueFromNodeLabel,omitempty"`

	// SourceNodeLabel is the node label to check for topology detection types.
	// Required when using arch, os, zone, region, or instance-type if you want
	// to check a non-standard label.
	// +optional
	SourceNodeLabel string `json:"sourceNodeLabel,omitempty"`

	// OnlyIfPresent if true, only applies the label if the resource/label exists.
	// If false (default), missing resources are treated as 0 quantity.
	// +optional
	OnlyIfPresent bool `json:"onlyIfPresent,omitempty"`
}

// NodeLabelSpec defines the desired state of NodeLabel.
type NodeLabelSpec struct {
	// NodeSelector is a map of key-value pairs used to select nodes.
	// Nodes matching ALL selectors will have the labels applied.
	// If empty, labels will be applied to ALL nodes in the cluster.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Labels is a map of static labels to apply to the selected nodes.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// AutoDetect contains rules for automatically detecting node capabilities
	// and applying labels based on the detection results.
	// +optional
	AutoDetect []AutoDetectRule `json:"autoDetect,omitempty"`

	// RemoveOnDelete specifies whether to remove the labels when this
	// NodeLabel resource is deleted. Defaults to false.
	// +optional
	RemoveOnDelete bool `json:"removeOnDelete,omitempty"`
}

// NodeLabelStatus defines the observed state of NodeLabel.
type NodeLabelStatus struct {
	// LabeledNodes is the list of node names that currently have the labels applied.
	// +optional
	LabeledNodes []string `json:"labeledNodes,omitempty"`

	// AppliedLabels stores the static labels that were last successfully applied.
	// +optional
	AppliedLabels map[string]string `json:"appliedLabels,omitempty"`

	// AutoDetectedLabels stores the auto-detected labels applied per node.
	// +optional
	AutoDetectedLabels map[string]map[string]string `json:"autoDetectedLabels,omitempty"`

	// AppliedNodeSelector stores the node selector that was last used.
	// +optional
	AppliedNodeSelector map[string]string `json:"appliedNodeSelector,omitempty"`

	// MatchingNodeCount is the total number of nodes matching the selector.
	// +optional
	MatchingNodeCount int `json:"matchingNodeCount,omitempty"`

	// LabeledNodeCount is the number of nodes that have been successfully labeled.
	// +optional
	LabeledNodeCount int `json:"labeledNodeCount,omitempty"`

	// GPUNodeCount is the number of nodes with GPU detected.
	// +optional
	GPUNodeCount int `json:"gpuNodeCount,omitempty"`

	// SRIOVNodeCount is the number of nodes with SR-IOV detected.
	// +optional
	SRIOVNodeCount int `json:"sriovNodeCount,omitempty"`

	// LastUpdated is the timestamp of the last successful reconciliation.
	// +optional
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`

	// Conditions represent the latest available observations of the NodeLabel's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Labeled",type="integer",JSONPath=".status.labeledNodeCount",description="Number of labeled nodes"
// +kubebuilder:printcolumn:name="Matching",type="integer",JSONPath=".status.matchingNodeCount",description="Number of matching nodes"
// +kubebuilder:printcolumn:name="GPUs",type="integer",JSONPath=".status.gpuNodeCount",description="Number of GPU nodes"
// +kubebuilder:printcolumn:name="SR-IOV",type="integer",JSONPath=".status.sriovNodeCount",description="Number of SR-IOV nodes"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// NodeLabel is the Schema for the nodelabels API.
type NodeLabel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeLabelSpec   `json:"spec,omitempty"`
	Status NodeLabelStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NodeLabelList contains a list of NodeLabel.
type NodeLabelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeLabel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeLabel{}, &NodeLabelList{})
}
