package kwok

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// ScaleConfig specifies the quantity and variety of mock Kubernetes resources to seed.
type ScaleConfig struct {
	Namespaces              []string `json:"namespaces" yaml:"namespaces"`
	NodesCount              int      `json:"nodes_count" yaml:"nodes_count"`
	PodsPerNamespace        int      `json:"pods_per_namespace" yaml:"pods_per_namespace"`
	DeploymentsPerNamespace int      `json:"deployments_per_namespace" yaml:"deployments_per_namespace"`
	ServicesPerNamespace    int      `json:"services_per_namespace" yaml:"services_per_namespace"`
	ConfigMapsPerNamespace  int      `json:"configmaps_per_namespace" yaml:"configmaps_per_namespace"`
	ClusterRolesCount       int      `json:"cluster_roles_count" yaml:"cluster_roles_count"`
	Concurrency             int      `json:"concurrency" yaml:"concurrency"`
}

// DefaultScaleConfig returns a balanced scale config suitable for fast automated testing (~500 resources).
func DefaultScaleConfig() ScaleConfig {
	return ScaleConfig{
		Namespaces:              []string{"default", "prod", "stage", "kube-system", "monitoring"},
		NodesCount:              10,
		PodsPerNamespace:        30,
		DeploymentsPerNamespace: 5,
		ServicesPerNamespace:    5,
		ConfigMapsPerNamespace:  5,
		ClusterRolesCount:       5,
		Concurrency:             20,
	}
}

// BenchmarkScaleConfig returns a heavy scale config (~5,000+ resources).
func BenchmarkScaleConfig() ScaleConfig {
	return ScaleConfig{
		Namespaces:              []string{"default", "prod", "stage", "kube-system", "monitoring", "analytics", "ingress"},
		NodesCount:              50,
		PodsPerNamespace:        300,
		DeploymentsPerNamespace: 50,
		ServicesPerNamespace:    50,
		ConfigMapsPerNamespace:  50,
		ClusterRolesCount:       30,
		Concurrency:             50,
	}
}

// WorkloadSeeder generates and inserts simulated resources into a Kubernetes cluster.
type WorkloadSeeder struct {
	dynClient dynamic.Interface
}

// NewWorkloadSeeder creates a new WorkloadSeeder from a rest.Config.
func NewWorkloadSeeder(restCfg *rest.Config) (*WorkloadSeeder, error) {
	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client for seeder: %w", err)
	}

	return &WorkloadSeeder{
		dynClient: dynClient,
	}, nil
}

// Seed populates the cluster with resources defined by the ScaleConfig.
func (s *WorkloadSeeder) Seed(
	ctx context.Context,
	cfg ScaleConfig,
) error {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}

	var totalCreated int64

	// 1. Namespaces
	nsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

	for _, nsName := range cfg.Namespaces {
		ns := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Namespace",
				"metadata": map[string]any{
					"name": nsName,
					"labels": map[string]any{
						"ksa.test/managed": "true",
					},
				},
			},
		}

		_, err := s.dynClient.Resource(nsGVR).Create(ctx, ns, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			slog.Warn(
				"Failed to create namespace during seeding",
				"namespace", nsName,
				"err", err,
			)
		} else {
			atomic.AddInt64(&totalCreated, 1)
		}
	}

	// 2. Nodes
	nodeGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}

	for i := 0; i < cfg.NodesCount; i++ {
		nodeName := fmt.Sprintf("kwok-node-%03d", i)
		node := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Node",
				"metadata": map[string]any{
					"name": nodeName,
					"labels": map[string]any{
						"type":             "kwok-node",
						"ksa.test/managed": "true",
					},
				},
				"status": map[string]any{
					"allocatable": map[string]any{
						"cpu":    "32",
						"memory": "128Gi",
						"pods":   "110",
					},
					"capacity": map[string]any{
						"cpu":    "32",
						"memory": "128Gi",
						"pods":   "110",
					},
					"phase": "Running",
				},
			},
		}

		_, err := s.dynClient.Resource(nodeGVR).Create(ctx, node, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			slog.Warn(
				"Failed to create node during seeding",
				"node", nodeName,
				"err", err,
			)
		} else {
			atomic.AddInt64(&totalCreated, 1)
		}
	}

	// 3. ClusterRoles (Cluster-scoped)
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}

	for i := 0; i < cfg.ClusterRolesCount; i++ {
		crName := fmt.Sprintf("ksa-test-role-%03d", i)
		cr := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "rbac.authorization.k8s.io/v1",
				"kind":       "ClusterRole",
				"metadata": map[string]any{
					"name": crName,
					"labels": map[string]any{
						"ksa.test/managed": "true",
					},
				},
				"rules": []any{
					map[string]any{
						"apiGroups": []any{""},
						"resources": []any{"pods"},
						"verbs":     []any{"get", "list", "watch"},
					},
				},
			},
		}

		_, err := s.dynClient.Resource(crGVR).Create(ctx, cr, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			slog.Warn(
				"Failed to create clusterrole during seeding",
				"clusterRole", crName,
				"err", err,
			)
		} else {
			atomic.AddInt64(&totalCreated, 1)
		}
	}

	// 4. Concurrent Namespaced Resources (Pods, Deployments, Services, ConfigMaps)
	type createJob struct {
		gvr       schema.GroupVersionResource
		namespace string
		obj       *unstructured.Unstructured
	}

	jobs := make(chan createJob, cfg.Concurrency*2)

	var wg sync.WaitGroup

	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for job := range jobs {
				_, err := s.dynClient.Resource(job.gvr).Namespace(job.namespace).Create(
					ctx,
					job.obj,
					metav1.CreateOptions{},
				)
				if err != nil && !errors.IsAlreadyExists(err) {
					slog.Debug(
						"Failed to create namespaced resource",
						"kind", job.obj.GetKind(),
						"namespace", job.namespace,
						"name", job.obj.GetName(),
						"err", err,
					)
				} else {
					atomic.AddInt64(&totalCreated, 1)
				}
			}
		}()
	}

	podGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	deployGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	svcGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
	cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	go func() {
		defer close(jobs)

		for _, ns := range cfg.Namespaces {
			// Pods
			for p := 0; p < cfg.PodsPerNamespace; p++ {
				podName := fmt.Sprintf("kwok-pod-%s-%04d", ns, p)
				pod := &unstructured.Unstructured{
					Object: map[string]any{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]any{
							"name":      podName,
							"namespace": ns,
							"labels": map[string]any{
								"app":              "test-app",
								"ksa.test/managed": "true",
								"instance":         fmt.Sprintf("pod-%d", p),
							},
							"annotations": map[string]any{
								"kwok.x-k8s.io/node": fmt.Sprintf("kwok-node-%03d", p%max(1, cfg.NodesCount)),
							},
						},
						"spec": map[string]any{
							"containers": []any{
								map[string]any{
									"name":  "mock-app",
									"image": "fake.registry.local/mock-app:v1",
								},
							},
						},
					},
				}

				jobs <- createJob{gvr: podGVR, namespace: ns, obj: pod}
			}

			// Deployments
			for d := 0; d < cfg.DeploymentsPerNamespace; d++ {
				depName := fmt.Sprintf("kwok-dep-%s-%03d", ns, d)
				dep := &unstructured.Unstructured{
					Object: map[string]any{
						"apiVersion": "apps/v1",
						"kind":       "Deployment",
						"metadata": map[string]any{
							"name":      depName,
							"namespace": ns,
							"labels": map[string]any{
								"ksa.test/managed": "true",
							},
						},
						"spec": map[string]any{
							"replicas": int64(3),
							"selector": map[string]any{
								"matchLabels": map[string]any{
									"app": depName,
								},
							},
							"template": map[string]any{
								"metadata": map[string]any{
									"labels": map[string]any{
										"app": depName,
									},
								},
								"spec": map[string]any{
									"containers": []any{
										map[string]any{
											"name":  "nginx",
											"image": "nginx:latest",
										},
									},
								},
							},
						},
					},
				}

				jobs <- createJob{gvr: deployGVR, namespace: ns, obj: dep}
			}

			// Services
			for s := 0; s < cfg.ServicesPerNamespace; s++ {
				svcName := fmt.Sprintf("kwok-svc-%s-%03d", ns, s)
				svc := &unstructured.Unstructured{
					Object: map[string]any{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]any{
							"name":      svcName,
							"namespace": ns,
							"labels": map[string]any{
								"ksa.test/managed": "true",
							},
						},
						"spec": map[string]any{
							"ports": []any{
								map[string]any{
									"name": "http",
									"port": int64(80),
								},
							},
							"type": "ClusterIP",
						},
					},
				}

				jobs <- createJob{gvr: svcGVR, namespace: ns, obj: svc}
			}

			// ConfigMaps
			for c := 0; c < cfg.ConfigMapsPerNamespace; c++ {
				cmName := fmt.Sprintf("kwok-cm-%s-%03d", ns, c)
				cm := &unstructured.Unstructured{
					Object: map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata": map[string]any{
							"name":      cmName,
							"namespace": ns,
							"labels": map[string]any{
								"ksa.test/managed": "true",
							},
						},
						"data": map[string]any{
							"key1": "value1",
							"key2": "value2",
						},
					},
				}

				jobs <- createJob{gvr: cmGVR, namespace: ns, obj: cm}
			}
		}
	}()

	wg.Wait()

	slog.Info(
		"Workload seeding complete",
		"totalCreated", atomic.LoadInt64(&totalCreated),
	)

	return nil
}
