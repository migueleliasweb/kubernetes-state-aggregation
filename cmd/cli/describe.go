package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func newDescribeCmd() *cobra.Command {
	var (
		descNamespace string
		descCluster   string
	)

	cmd := &cobra.Command{
		Use:   "describe <kind> <name>",
		Short: "Show details of a specific resource across clusters",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			kind := args[0]
			name := args[1]

			conn, err := grpc.NewClient(
				serverAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return fmt.Errorf("failed to connect to gRPC server at %s: %w", serverAddr, err)
			}
			defer conn.Close()

			client := v1.NewStateServiceClient(conn)
			ctx := context.Background()

			res, err := client.ListResources(ctx, &v1.ListResourcesRequest{
				Filter: &v1.ResourceInfo{
					ClusterName: descCluster,
					Kind:        kind,
					Name:        name,
					Namespace:   descNamespace,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to describe resource: %w", err)
			}

			if len(res.Items) == 0 {
				return fmt.Errorf("resource %s/%s not found", kind, name)
			}

			for i, item := range res.Items {
				if i > 0 {
					fmt.Println("\n" + strings.Repeat("-", 60) + "\n")
				}

				describeResource(item)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&descNamespace, "namespace", "n", "", "Optional Kubernetes namespace filter")
	cmd.Flags().StringVarP(&descCluster, "cluster", "c", "", "Optional cluster name filter")

	return cmd
}

func describeResource(item *v1.ResourceRecord) {
	fmt.Printf("Name:         %s\n", item.Name)
	if item.Namespace != "" {
		fmt.Printf("Namespace:    %s\n", item.Namespace)
	}
	fmt.Printf("Cluster:      %s\n", item.ClusterName)
	fmt.Printf("Kind:         %s\n", item.Kind)
	if item.GroupName != "" || item.Version != "" {
		apiVersion := item.Version
		if item.GroupName != "" {
			apiVersion = fmt.Sprintf("%s/%s", item.GroupName, item.Version)
		}
		fmt.Printf("API Version:  %s\n", apiVersion)
	}
	fmt.Printf("UID:          %s\n", item.Uid)
	if item.ResourceVersion != "" {
		fmt.Printf("Resource Ver: %s\n", item.ResourceVersion)
	}
	if item.UpdatedAt != nil {
		fmt.Printf("Updated At:   %s (%s ago)\n",
			item.UpdatedAt.AsTime().Format(time.RFC3339),
			time.Since(item.UpdatedAt.AsTime()).Round(time.Second).String(),
		)
	}

	// Labels
	labels := parseMap(item.Labels)
	if len(labels) == 0 {
		fmt.Println("Labels:       <none>")
	} else {
		fmt.Println("Labels:")
		for k, v := range labels {
			fmt.Printf("              %s=%s\n", k, v)
		}
	}

	// Annotations
	annotations := parseMap(item.Annotations)
	if len(annotations) == 0 {
		fmt.Println("Annotations:  <none>")
	} else {
		fmt.Println("Annotations:")
		for k, v := range annotations {
			fmt.Printf("              %s=%s\n", k, v)
		}
	}

	if len(item.Manifest) == 0 {
		return
	}

	var u unstructured.Unstructured
	if err := json.Unmarshal(item.Manifest, &u.Object); err != nil {
		return
	}

	// Owner References
	ownerRefs := u.GetOwnerReferences()
	if len(ownerRefs) > 0 {
		fmt.Println("Controlled By:")
		for _, ref := range ownerRefs {
			fmt.Printf("              %s/%s (%s)\n", ref.Kind, ref.Name, ref.UID)
		}
	}

	// Type specific formatting
	switch item.Kind {
	case "Pod":
		describePod(&u)
	case "Deployment":
		describeDeployment(&u)
	case "Service":
		describeService(&u)
	default:
		describeGeneric(&u)
	}
}

func describePod(u *unstructured.Unstructured) {
	obj := u.Object

	status, _ := obj["status"].(map[string]any)
	spec, _ := obj["spec"].(map[string]any)

	if status != nil {
		phase, _ := status["phase"].(string)
		fmt.Printf("Status:       %s\n", phase)
		if podIP, ok := status["podIP"].(string); ok && podIP != "" {
			fmt.Printf("IP:           %s\n", podIP)
		}
	}

	if spec != nil {
		if nodeName, ok := spec["nodeName"].(string); ok && nodeName != "" {
			fmt.Printf("Node:         %s\n", nodeName)
		}

		if containers, ok := spec["containers"].([]any); ok {
			fmt.Println("Containers:")
			for _, c := range containers {
				cMap, _ := c.(map[string]any)
				if cMap == nil {
					continue
				}
				cName, _ := cMap["name"].(string)
				cImage, _ := cMap["image"].(string)
				fmt.Printf("  %s:\n", cName)
				fmt.Printf("    Image:    %s\n", cImage)

				if ports, ok := cMap["ports"].([]any); ok && len(ports) > 0 {
					var portStrs []string
					for _, p := range ports {
						pMap, _ := p.(map[string]any)
						if pMap != nil {
							portStrs = append(portStrs, fmt.Sprintf("%v/%v", pMap["containerPort"], pMap["protocol"]))
						}
					}
					fmt.Printf("    Ports:    %s\n", strings.Join(portStrs, ", "))
				}
			}
		}
	}

	if status != nil {
		if conditions, ok := status["conditions"].([]any); ok && len(conditions) > 0 {
			fmt.Println("Conditions:")
			fmt.Println("  Type\t\tStatus")
			for _, cond := range conditions {
				condMap, _ := cond.(map[string]any)
				if condMap != nil {
					fmt.Printf("  %s\t%s\n", condMap["type"], condMap["status"])
				}
			}
		}
	}
}

func describeDeployment(u *unstructured.Unstructured) {
	obj := u.Object
	spec, _ := obj["spec"].(map[string]any)
	status, _ := obj["status"].(map[string]any)

	if spec != nil {
		replicas := spec["replicas"]
		fmt.Printf("Replicas:     %v desired\n", replicas)

		if selector, ok := spec["selector"].(map[string]any); ok {
			if matchLabels, ok := selector["matchLabels"].(map[string]any); ok {
				var pairs []string
				for k, v := range matchLabels {
					pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
				}
				fmt.Printf("Selector:     %s\n", strings.Join(pairs, ","))
			}
		}

		if strategy, ok := spec["strategy"].(map[string]any); ok {
			if sType, ok := strategy["type"].(string); ok {
				fmt.Printf("Strategy:     %s\n", sType)
			}
		}
	}

	if status != nil {
		ready, _ := status["readyReplicas"]
		updated, _ := status["updatedReplicas"]
		available, _ := status["availableReplicas"]
		fmt.Printf("Status:       %v updated, %v ready, %v available\n", updated, ready, available)

		if conditions, ok := status["conditions"].([]any); ok && len(conditions) > 0 {
			fmt.Println("Conditions:")
			for _, cond := range conditions {
				condMap, _ := cond.(map[string]any)
				if condMap != nil {
					reason, _ := condMap["reason"].(string)
					msg, _ := condMap["message"].(string)
					fmt.Printf("  %s: %s (%s - %s)\n", condMap["type"], condMap["status"], reason, msg)
				}
			}
		}
	}
}

func describeService(u *unstructured.Unstructured) {
	obj := u.Object
	spec, _ := obj["spec"].(map[string]any)

	if spec != nil {
		if sType, ok := spec["type"].(string); ok {
			fmt.Printf("Type:         %s\n", sType)
		}
		if clusterIP, ok := spec["clusterIP"].(string); ok {
			fmt.Printf("ClusterIP:    %s\n", clusterIP)
		}
		if selector, ok := spec["selector"].(map[string]any); ok {
			var pairs []string
			for k, v := range selector {
				pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
			}
			fmt.Printf("Selector:     %s\n", strings.Join(pairs, ","))
		}
		if ports, ok := spec["ports"].([]any); ok && len(ports) > 0 {
			fmt.Println("Port(s):")
			for _, p := range ports {
				pMap, _ := p.(map[string]any)
				if pMap != nil {
					pName, _ := pMap["name"].(string)
					fmt.Printf("              %s %v/%v -> %v\n", pName, pMap["port"], pMap["protocol"], pMap["targetPort"])
				}
			}
		}
	}
}

func describeGeneric(u *unstructured.Unstructured) {
	obj := u.Object
	if spec, ok := obj["spec"]; ok && spec != nil {
		specYAML, err := yaml.Marshal(spec)
		if err == nil {
			fmt.Println("Spec:")
			for _, line := range strings.Split(strings.TrimSpace(string(specYAML)), "\n") {
				fmt.Printf("  %s\n", line)
			}
		}
	}

	if status, ok := obj["status"]; ok && status != nil {
		statusYAML, err := yaml.Marshal(status)
		if err == nil {
			fmt.Println("Status:")
			for _, line := range strings.Split(strings.TrimSpace(string(statusYAML)), "\n") {
				fmt.Printf("  %s\n", line)
			}
		}
	}
}

func parseMap(data []byte) map[string]string {
	if len(data) == 0 {
		return nil
	}

	res := map[string]string{}
	_ = json.Unmarshal(data, &res)

	return res
}
