package main

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	namespace string
	cluster   string
)

func newGraphCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph <kind> <name>",
		Short: "Display an ASCII tree graph of a Kubernetes resource and its relationships",
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

			res, err := client.FetchResourceGraph(ctx, &v1.FetchResourceGraphRequest{
				Root: &v1.ResourceInfo{
					ClusterName: cluster,
					Kind:        kind,
					Name:        name,
					Namespace:   namespace,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to fetch resource graph: %w", err)
			}

			if len(res.Items) == 0 {
				fmt.Println("No resources found matching the criteria.")
				return nil
			}

			printGraph(res.Items)

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Optional Kubernetes namespace filter")
	cmd.Flags().StringVarP(&cluster, "cluster", "c", "", "Optional cluster name filter")

	return cmd
}

type node struct {
	record   *v1.ResourceRecord
	children []*node
}

func printGraph(items []*v1.ResourceRecord) {
	// Build a map of UID to *node
	nodes := map[string]*node{}
	for _, item := range items {
		nodes[item.Uid] = &node{
			record: item,
		}
	}

	// Identify roots and build the tree
	isChild := map[string]bool{}
	for _, n := range nodes {
		if len(n.record.Manifest) > 0 {
			var u unstructured.Unstructured
			if err := json.Unmarshal(n.record.Manifest, &u.Object); err == nil {
				for _, ownerRef := range u.GetOwnerReferences() {
					parentUID := string(ownerRef.UID)
					if parentNode, exists := nodes[parentUID]; exists {
						parentNode.children = append(parentNode.children, n)
						isChild[n.record.Uid] = true
					}
				}
			}
		}
	}

	// Find the roots (nodes that are not children of any other node in the collection)
	roots := []*node{}
	for _, n := range nodes {
		if !isChild[n.record.Uid] {
			roots = append(roots, n)
		}
	}

	// Print the tree
	for i, root := range roots {
		if i > 0 {
			fmt.Println()
		}

		nsStr := ""
		if root.record.Namespace != "" {
			nsStr = fmt.Sprintf("%s/", root.record.Namespace)
		}
		clusterStr := ""
		if root.record.ClusterName != "" {
			clusterStr = fmt.Sprintf(" [%s]", root.record.ClusterName)
		}

		fmt.Printf("%s %s%s%s\n", root.record.Kind, nsStr, root.record.Name, clusterStr)
		for j, child := range root.children {
			printNode(child, "", j == len(root.children)-1)
		}
	}
}

func printNode(n *node, prefix string, isLast bool) {
	fmt.Printf("%s", prefix)
	if isLast {
		fmt.Print("└── ")
		prefix += "    "
	} else {
		fmt.Print("├── ")
		prefix += "│   "
	}

	nsStr := ""
	if n.record.Namespace != "" {
		nsStr = fmt.Sprintf("%s/", n.record.Namespace)
	}
	clusterStr := ""
	if n.record.ClusterName != "" {
		clusterStr = fmt.Sprintf(" [%s]", n.record.ClusterName)
	}

	fmt.Printf("%s %s%s%s\n", n.record.Kind, nsStr, n.record.Name, clusterStr)

	for i, child := range n.children {
		printNode(child, prefix, i == len(n.children)-1)
	}
}
