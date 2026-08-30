package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore"
	"github.com/migueleliasweb/kubernetes-state-aggregation/pkg/datastore/postgres"
	"github.com/spf13/cobra"
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

			store, err := postgres.NewPGSyncer(dbURL)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %w", err)
			}
			defer store.Close()

			ctx := context.Background()

			rootInfo := datastore.ResourceInfo{
				Kind:        kind,
				Name:        name,
				Namespace:   namespace,
				ClusterName: cluster,
			}

			collection, err := store.FetchResourceGraph(ctx, rootInfo, func(resourceInfo datastore.ResourceRecord) (datastore.WalkAction, error) {
				return datastore.ActionInclude, nil
			})
			if err != nil {
				return fmt.Errorf("failed to fetch resource graph: %w", err)
			}

			items := collection.Items()
			if len(items) == 0 {
				fmt.Println("No resources found matching the criteria.")
				return nil
			}

			printGraph(items)

			return nil
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Optional Kubernetes namespace filter")
	cmd.Flags().StringVarP(&cluster, "cluster", "c", "", "Optional cluster name filter")

	return cmd
}

type node struct {
	record   datastore.ResourceRecord
	children []*node
}

func printGraph(items []datastore.ResourceRecord) {
	// Build a map of UID to *node
	nodes := make(map[string]*node)
	for _, item := range items {
		nodes[item.UID] = &node{
			record: item,
		}
	}

	// Identify roots and build the tree
	isChild := make(map[string]bool)
	for _, n := range nodes {
		if n.record.Manifest != nil {
			var u unstructured.Unstructured
			if err := json.Unmarshal(n.record.Manifest, &u.Object); err == nil {
				for _, ownerRef := range u.GetOwnerReferences() {
					parentUID := string(ownerRef.UID)
					if parentNode, exists := nodes[parentUID]; exists {
						parentNode.children = append(parentNode.children, n)
						isChild[n.record.UID] = true
					}
				}
			}
		}
	}

	// Find the roots (nodes that are not children of any other node in the collection)
	var roots []*node
	for _, n := range nodes {
		if !isChild[n.record.UID] {
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
