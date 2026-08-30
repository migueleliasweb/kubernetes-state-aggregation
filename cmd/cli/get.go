package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	v1 "github.com/migueleliasweb/kubernetes-state-aggregation/pkg/api/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

func newGetCmd() *cobra.Command {
	var (
		getNamespace string
		getCluster   string
		outputFormat string
	)

	cmd := &cobra.Command{
		Use:   "get <kind> [name]",
		Short: "Display one or many resources from the aggregated state",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			kind := args[0]
			var name string
			if len(args) > 1 {
				name = args[1]
			}

			// Default output format
			if outputFormat == "" {
				if name != "" {
					outputFormat = "yaml"
				} else {
					outputFormat = "table"
				}
			}

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
					ClusterName: getCluster,
					Kind:        kind,
					Name:        name,
					Namespace:   getNamespace,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to get resources: %w", err)
			}

			if len(res.Items) == 0 {
				if name != "" {
					return fmt.Errorf("resource %s/%s not found", kind, name)
				}

				fmt.Println("No resources found matching the criteria.")

				return nil
			}

			switch outputFormat {
			case "json":
				return printJSON(res.Items)
			case "yaml":
				return printYAML(res.Items)
			case "table":
				return printTable(res.Items)
			default:
				return fmt.Errorf("unsupported output format %q (supported: yaml, json, table)", outputFormat)
			}
		},
	}

	cmd.Flags().StringVarP(&getNamespace, "namespace", "n", "", "Optional Kubernetes namespace filter")
	cmd.Flags().StringVarP(&getCluster, "cluster", "c", "", "Optional cluster name filter")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", "", "Output format (yaml, json, table)")

	return cmd
}

func printTable(items []*v1.ResourceRecord) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tNAMESPACE\tNAME\tKIND\tAGE")

	for _, item := range items {
		ns := item.Namespace
		if ns == "" {
			ns = "<cluster-scoped>"
		}

		ageStr := "-"
		if item.UpdatedAt != nil {
			age := time.Since(item.UpdatedAt.AsTime()).Round(time.Second)
			ageStr = age.String()
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.ClusterName, ns, item.Name, item.Kind, ageStr)
	}

	return w.Flush()
}

func printYAML(items []*v1.ResourceRecord) error {
	for i, item := range items {
		if len(items) > 1 {
			if i > 0 {
				fmt.Println("---")
			}
			fmt.Printf("# Cluster: %s\n", item.ClusterName)
		}

		var obj any
		if len(item.Manifest) > 0 {
			if err := json.Unmarshal(item.Manifest, &obj); err != nil {
				return fmt.Errorf("failed to parse resource manifest: %w", err)
			}
		} else {
			obj = map[string]any{
				"kind": item.Kind,
				"metadata": map[string]any{
					"name":      item.Name,
					"namespace": item.Namespace,
				},
			}
		}

		yamlBytes, err := yaml.Marshal(obj)
		if err != nil {
			return fmt.Errorf("failed to format YAML: %w", err)
		}

		fmt.Print(string(yamlBytes))
	}

	return nil
}

func printJSON(items []*v1.ResourceRecord) error {
	if len(items) == 1 {
		var obj any
		if err := json.Unmarshal(items[0].Manifest, &obj); err != nil {
			return fmt.Errorf("failed to parse resource manifest: %w", err)
		}

		buf := new(bytes.Buffer)
		enc := json.NewEncoder(buf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(obj); err != nil {
			return err
		}

		fmt.Print(buf.String())

		return nil
	}

	var allObjs []any
	for _, item := range items {
		var obj any
		if err := json.Unmarshal(item.Manifest, &obj); err != nil {
			return fmt.Errorf("failed to parse resource manifest: %w", err)
		}
		allObjs = append(allObjs, obj)
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(allObjs); err != nil {
		return err
	}

	fmt.Print(buf.String())

	return nil
}
