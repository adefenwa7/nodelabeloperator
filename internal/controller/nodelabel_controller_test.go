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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodelabelv1alpha1 "github.com/adefenwa7/node-labeler-operator/api/v1alpha1"
)

var _ = Describe("NodeLabel Controller", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	// Helper function to create a reconciler
	createReconciler := func() *NodeLabelReconciler {
		return &NodeLabelReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
	}

	// Helper function to cleanup a NodeLabel
	cleanupNodeLabel := func(ctx context.Context, name string) {
		nl := &nodelabelv1alpha1.NodeLabel{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, nl)
		if err == nil {
			nl.Finalizers = nil
			_ = k8sClient.Update(ctx, nl)
			_ = k8sClient.Delete(ctx, nl)
		}
	}

	// Helper function to cleanup a Node
	cleanupNode := func(ctx context.Context, name string) {
		node := &corev1.Node{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, node)
		if err == nil {
			_ = k8sClient.Delete(ctx, node)
		}
	}

	Context("Static Labels", func() {
		const resourceName = "test-static-labels"
		const nodeName = "test-static-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						"node-role.kubernetes.io/worker": "",
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{"node-role.kubernetes.io/worker": ""},
					Labels: map[string]string{
						"environment": "test",
						"managed-by":  "operator",
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, nodeName)
		})

		It("should apply static labels to matching nodes", func() {
			reconciler := createReconciler()

			// Reconcile twice (finalizer + labels)
			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("environment", "test"))
			Expect(node.Labels).To(HaveKeyWithValue("managed-by", "operator"))
		})
	})

	Context("GPU Detection", func() {
		const resourceName = "test-gpu-detection"
		const gpuNodeName = "test-gpu-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: gpuNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                    resource.MustParse("8"),
						corev1.ResourceMemory:                 resource.MustParse("32Gi"),
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
					},
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU:                    resource.MustParse("8"),
						corev1.ResourceMemory:                 resource.MustParse("32Gi"),
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: gpuNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectGPU, LabelKey: "gpu", LabelValue: "present"},
						{Type: nodelabelv1alpha1.AutoDetectNvidiaGPU, LabelKey: "nvidia-gpu-count", LabelValueFromQuantity: true},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, gpuNodeName)
		})

		It("should detect NVIDIA GPU and apply labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: gpuNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("gpu", "present"))
			Expect(node.Labels).To(HaveKeyWithValue("nvidia-gpu-count", "4"))

			nl := &nodelabelv1alpha1.NodeLabel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, nl)).To(Succeed())
			Expect(nl.Status.GPUNodeCount).To(Equal(1))
		})
	})

	Context("AMD GPU Detection", func() {
		const resourceName = "test-amd-gpu-detection"
		const amdGpuNodeName = "test-amd-gpu-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: amdGpuNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                 resource.MustParse("16"),
						corev1.ResourceMemory:              resource.MustParse("64Gi"),
						corev1.ResourceName("amd.com/gpu"): resource.MustParse("2"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: amdGpuNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectAMDGPU, LabelKey: "amd-gpu", LabelValue: "available"},
						{Type: nodelabelv1alpha1.AutoDetectGPU, LabelKey: "any-gpu", LabelValueFromQuantity: true},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, amdGpuNodeName)
		})

		It("should detect AMD GPU and apply labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: amdGpuNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("amd-gpu", "available"))
			Expect(node.Labels).To(HaveKeyWithValue("any-gpu", "2"))
		})
	})

	Context("Memory and CPU Detection", func() {
		const resourceName = "test-resource-detection"
		const highResourceNodeName = "test-high-resource-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: highResourceNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("32"),
						corev1.ResourceMemory: resource.MustParse("128Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: highResourceNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			minMem := resource.MustParse("64Gi")
			minCPU := resource.MustParse("16")
			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectMemory, MinQuantity: &minMem, LabelKey: "high-memory", LabelValue: "true"},
						{Type: nodelabelv1alpha1.AutoDetectCPU, MinQuantity: &minCPU, LabelKey: "high-cpu", LabelValue: "true"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, highResourceNodeName)
		})

		It("should detect high memory and CPU nodes", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: highResourceNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("high-memory", "true"))
			Expect(node.Labels).To(HaveKeyWithValue("high-cpu", "true"))
		})
	})

	Context("Ephemeral Storage Detection", func() {
		const resourceName = "test-storage-detection"
		const storageNodeName = "test-storage-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: storageNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:              resource.MustParse("4"),
						corev1.ResourceMemory:           resource.MustParse("16Gi"),
						corev1.ResourceEphemeralStorage: resource.MustParse("500Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: storageNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			minStorage := resource.MustParse("200Gi")
			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectEphemeralStorage, MinQuantity: &minStorage, LabelKey: "high-storage", LabelValue: "true"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, storageNodeName)
		})

		It("should detect high ephemeral storage nodes", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: storageNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("high-storage", "true"))
		})
	})

	Context("Pod Capacity Detection", func() {
		const resourceName = "test-pod-detection"
		const podNodeName = "test-pod-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: podNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("8"),
						corev1.ResourceMemory: resource.MustParse("32Gi"),
						corev1.ResourcePods:   resource.MustParse("250"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: podNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			minPods := resource.MustParse("200")
			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectPods, MinQuantity: &minPods, LabelKey: "high-pod-capacity", LabelValue: "true"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, podNodeName)
		})

		It("should detect high pod capacity nodes", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("high-pod-capacity", "true"))
		})
	})

	Context("Hugepages Detection", func() {
		const resourceName = "test-hugepages-detection"
		const hugepagesNodeName = "test-hugepages-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: hugepagesNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                   resource.MustParse("8"),
						corev1.ResourceMemory:                resource.MustParse("32Gi"),
						corev1.ResourceName("hugepages-2Mi"): resource.MustParse("1Gi"),
						corev1.ResourceName("hugepages-1Gi"): resource.MustParse("4Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: hugepagesNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectHugepages2Mi, LabelKey: "hugepages-2mi", LabelValue: "available"},
						{Type: nodelabelv1alpha1.AutoDetectHugepages1Gi, LabelKey: "hugepages-1gi", LabelValue: "available"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, hugepagesNodeName)
		})

		It("should detect hugepages and apply labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: hugepagesNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("hugepages-2mi", "available"))
			Expect(node.Labels).To(HaveKeyWithValue("hugepages-1gi", "available"))
		})
	})

	Context("Topology Detection (Arch, OS, Zone, Region)", func() {
		const resourceName = "test-topology-detection"
		const topologyNodeName = "test-topology-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: topologyNodeName,
					Labels: map[string]string{
						"kubernetes.io/arch":               "amd64",
						"kubernetes.io/os":                 "linux",
						"topology.kubernetes.io/zone":      "us-east-1a",
						"topology.kubernetes.io/region":    "us-east-1",
						"node.kubernetes.io/instance-type": "m5.xlarge",
					},
				},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("4"),
						corev1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: topologyNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectArch, LabelKey: "cpu-arch"},
						{Type: nodelabelv1alpha1.AutoDetectOS, LabelKey: "node-os"},
						{Type: nodelabelv1alpha1.AutoDetectZone, LabelKey: "availability-zone"},
						{Type: nodelabelv1alpha1.AutoDetectRegion, LabelKey: "cloud-region"},
						{Type: nodelabelv1alpha1.AutoDetectInstanceType, LabelKey: "vm-type"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, topologyNodeName)
		})

		It("should detect topology labels and copy values", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: topologyNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("cpu-arch", "amd64"))
			Expect(node.Labels).To(HaveKeyWithValue("node-os", "linux"))
			Expect(node.Labels).To(HaveKeyWithValue("availability-zone", "us-east-1a"))
			Expect(node.Labels).To(HaveKeyWithValue("cloud-region", "us-east-1"))
			Expect(node.Labels).To(HaveKeyWithValue("vm-type", "m5.xlarge"))
		})
	})

	Context("Topology Detection with OnlyIfPresent", func() {
		const resourceName = "test-topology-onlyifpresent"
		const noZoneNodeName = "test-no-zone-node"
		ctx := context.Background()

		BeforeEach(func() {
			// Node without zone/region labels
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: noZoneNodeName,
					Labels: map[string]string{
						"kubernetes.io/arch": "arm64",
						"kubernetes.io/os":   "linux",
						// No zone or region labels
					},
				},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("4"),
						corev1.ResourceMemory: resource.MustParse("16Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: noZoneNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectArch, LabelKey: "cpu-arch"},
						{Type: nodelabelv1alpha1.AutoDetectZone, LabelKey: "zone", OnlyIfPresent: true},
						{Type: nodelabelv1alpha1.AutoDetectRegion, LabelKey: "region", OnlyIfPresent: true},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, noZoneNodeName)
		})

		It("should only apply labels when source labels are present", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: noZoneNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("cpu-arch", "arm64"))
			// Zone and region should NOT be present since source labels don't exist
			Expect(node.Labels).NotTo(HaveKey("zone"))
			Expect(node.Labels).NotTo(HaveKey("region"))
		})
	})

	Context("SR-IOV Detection", func() {
		const resourceName = "test-sriov-detection"
		const sriovNodeName = "test-sriov-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: sriovNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                               resource.MustParse("16"),
						corev1.ResourceMemory:                            resource.MustParse("64Gi"),
						corev1.ResourceName("intel.com/sriov_netdevice"): resource.MustParse("8"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: sriovNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectSRIOV, LabelKey: "sriov", LabelValue: "capable"},
						{Type: nodelabelv1alpha1.AutoDetectSRIOV, ResourceName: "intel.com/sriov_netdevice", LabelKey: "sriov-devices", LabelValueFromQuantity: true},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, sriovNodeName)
		})

		It("should detect SR-IOV and apply labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: sriovNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("sriov", "capable"))
			Expect(node.Labels).To(HaveKeyWithValue("sriov-devices", "8"))

			nl := &nodelabelv1alpha1.NodeLabel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, nl)).To(Succeed())
			Expect(nl.Status.SRIOVNodeCount).To(Equal(1))
		})
	})

	Context("Custom Resource Detection", func() {
		const resourceName = "test-custom-detection"
		const customNodeName = "test-custom-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: customNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                        resource.MustParse("8"),
						corev1.ResourceMemory:                     resource.MustParse("32Gi"),
						corev1.ResourceName("xilinx.com/fpga"):    resource.MustParse("2"),
						corev1.ResourceName("example.com/custom"): resource.MustParse("5"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: customNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectCustom, ResourceName: "xilinx.com/fpga", LabelKey: "fpga", LabelValue: "present"},
						{Type: nodelabelv1alpha1.AutoDetectCustom, ResourceName: "example.com/custom", LabelKey: "custom-resource", LabelValueFromQuantity: true},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, customNodeName)
		})

		It("should detect custom resources and apply labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: customNodeName}, node)).To(Succeed())
			Expect(node.Labels).To(HaveKeyWithValue("fpga", "present"))
			Expect(node.Labels).To(HaveKeyWithValue("custom-resource", "5"))
		})
	})

	Context("Combined Static and Auto-Detect Labels", func() {
		const resourceName = "test-combined-detection"
		const combinedNodeName = "test-combined-node"
		ctx := context.Background()

		BeforeEach(func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: combinedNodeName,
					Labels: map[string]string{
						"kubernetes.io/arch": "amd64",
					},
				},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:                    resource.MustParse("32"),
						corev1.ResourceMemory:                 resource.MustParse("128Gi"),
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: combinedNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			minMem := resource.MustParse("64Gi")
			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					Labels: map[string]string{
						"managed-by": "node-labeler-operator",
						"env":        "production",
					},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectGPU, LabelKey: "gpu", LabelValue: "present"},
						{Type: nodelabelv1alpha1.AutoDetectMemory, MinQuantity: &minMem, LabelKey: "high-memory", LabelValue: "true"},
						{Type: nodelabelv1alpha1.AutoDetectArch, LabelKey: "arch"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, combinedNodeName)
		})

		It("should apply both static and auto-detected labels", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: combinedNodeName}, node)).To(Succeed())

			// Static labels
			Expect(node.Labels).To(HaveKeyWithValue("managed-by", "node-labeler-operator"))
			Expect(node.Labels).To(HaveKeyWithValue("env", "production"))

			// Auto-detected labels
			Expect(node.Labels).To(HaveKeyWithValue("gpu", "present"))
			Expect(node.Labels).To(HaveKeyWithValue("high-memory", "true"))
			Expect(node.Labels).To(HaveKeyWithValue("arch", "amd64"))

			// Check status
			nl := &nodelabelv1alpha1.NodeLabel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, nl)).To(Succeed())
			Expect(nl.Status.GPUNodeCount).To(Equal(1))
			Expect(nl.Status.AutoDetectedLabels[combinedNodeName]).To(HaveKeyWithValue("gpu", "present"))
		})
	})

	Context("No Matching Resources", func() {
		const resourceName = "test-no-match-detection"
		const noResourceNodeName = "test-no-resource-node"
		ctx := context.Background()

		BeforeEach(func() {
			// Node with minimal resources, no GPUs
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: noResourceNodeName},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: noResourceNodeName}, node)
			if errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
			}

			minMem := resource.MustParse("64Gi")
			nl := &nodelabelv1alpha1.NodeLabel{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName},
				Spec: nodelabelv1alpha1.NodeLabelSpec{
					NodeSelector: map[string]string{},
					AutoDetect: []nodelabelv1alpha1.AutoDetectRule{
						{Type: nodelabelv1alpha1.AutoDetectGPU, LabelKey: "gpu", LabelValue: "present"},
						{Type: nodelabelv1alpha1.AutoDetectMemory, MinQuantity: &minMem, LabelKey: "high-memory", LabelValue: "true"},
					},
					RemoveOnDelete: true,
				},
			}
			Expect(k8sClient.Create(ctx, nl)).To(Succeed())
		})

		AfterEach(func() {
			cleanupNodeLabel(ctx, resourceName)
			cleanupNode(ctx, noResourceNodeName)
		})

		It("should not apply labels when resources don't meet thresholds", func() {
			reconciler := createReconciler()

			_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: resourceName}})
			Expect(err).NotTo(HaveOccurred())

			node := &corev1.Node{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: noResourceNodeName}, node)).To(Succeed())

			// These labels should NOT be present
			Expect(node.Labels).NotTo(HaveKey("gpu"))
			Expect(node.Labels).NotTo(HaveKey("high-memory"))

			// Check status shows no GPU nodes
			nl := &nodelabelv1alpha1.NodeLabel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName}, nl)).To(Succeed())
			Expect(nl.Status.GPUNodeCount).To(Equal(0))
		})
	})
})
