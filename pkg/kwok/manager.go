package kwok

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ClusterManager manages KWOK (Kubernetes WithOut Kubelet) simulated clusters.
type ClusterManager struct {
	kwokctlPath string
}

// NewClusterManager creates a new KWOK cluster manager.
func NewClusterManager() (*ClusterManager, error) {
	kwokctlPath, err := exec.LookPath("kwokctl")
	if err != nil {
		homeDir, _ := os.UserHomeDir()
		fallbackPaths := []string{
			filepath.Join(homeDir, ".kwok", "bin", "kwokctl"),
			filepath.Join(homeDir, "go", "bin", "kwokctl"),
			"/usr/local/bin/kwokctl",
			"/opt/homebrew/bin/kwokctl",
		}

		for _, p := range fallbackPaths {
			if _, statErr := os.Stat(p); statErr == nil {
				kwokctlPath = p
				err = nil

				break
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf(
			"kwokctl binary not found in PATH or standard locations. Please install kwokctl: https://kwok.sigs.k8s.io/docs/user/installation/",
		)
	}

	return &ClusterManager{
		kwokctlPath: kwokctlPath,
	}, nil
}

// CreateCluster spins up a simulated cluster using the binary runtime.
func (m *ClusterManager) CreateCluster(
	ctx context.Context,
	name string,
) error {
	slog.Info(
		"Creating KWOK cluster",
		"cluster", name,
		"runtime", "binary",
	)

	cmd := exec.CommandContext(
		ctx,
		m.kwokctlPath,
		"create",
		"cluster",
		"--name", name,
		"--runtime", "binary",
	)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to create KWOK cluster %q: %w (stderr: %s)",
			name,
			err,
			errBuf.String(),
		)
	}

	slog.Info(
		"KWOK cluster created successfully",
		"cluster", name,
	)

	return nil
}

// DeleteCluster tears down a simulated cluster.
func (m *ClusterManager) DeleteCluster(
	ctx context.Context,
	name string,
) error {
	slog.Info(
		"Deleting KWOK cluster",
		"cluster", name,
	)

	cmd := exec.CommandContext(
		ctx,
		m.kwokctlPath,
		"delete",
		"cluster",
		"--name", name,
	)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to delete KWOK cluster %q: %w (stderr: %s)",
			name,
			err,
			errBuf.String(),
		)
	}

	slog.Info(
		"KWOK cluster deleted successfully",
		"cluster", name,
	)

	return nil
}

// GetKubeconfig retrieves the kubeconfig content for the named cluster.
func (m *ClusterManager) GetKubeconfig(
	ctx context.Context,
	name string,
) ([]byte, error) {
	cmd := exec.CommandContext(
		ctx,
		m.kwokctlPath,
		"get",
		"kubeconfig",
		"--name", name,
	)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf(
			"failed to get kubeconfig for cluster %q: %w (stderr: %s)",
			name,
			err,
			errBuf.String(),
		)
	}

	return outBuf.Bytes(), nil
}

// GetKubeconfigPath returns the path to the cluster's kubeconfig file on disk.
func (m *ClusterManager) GetKubeconfigPath(
	ctx context.Context,
	name string,
	targetDir string,
) (string, error) {
	data, err := m.GetKubeconfig(ctx, name)
	if err != nil {
		return "", err
	}

	if targetDir == "" {
		targetDir = os.TempDir()
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", targetDir, err)
	}

	filePath := filepath.Join(targetDir, fmt.Sprintf("kwok-%s-kubeconfig.yaml", name))

	if err := os.WriteFile(filePath, data, 0o600); err != nil {
		return "", fmt.Errorf("failed to write kubeconfig to %s: %w", filePath, err)
	}

	return filePath, nil
}

// GetRESTConfig builds a client-go *rest.Config from the cluster's kubeconfig.
func (m *ClusterManager) GetRESTConfig(
	ctx context.Context,
	name string,
) (*rest.Config, error) {
	data, err := m.GetKubeconfig(ctx, name)
	if err != nil {
		return nil, err
	}

	clientCfg, err := clientcmd.NewClientConfigFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kubeconfig for cluster %q: %w", name, err)
	}

	restCfg, err := clientCfg.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build REST config for cluster %q: %w", name, err)
	}

	restCfg.QPS = 500
	restCfg.Burst = 1000

	return restCfg, nil
}

// ListClusters returns a list of active KWOK cluster names.
func (m *ClusterManager) ListClusters(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		m.kwokctlPath,
		"get",
		"clusters",
	)

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer

	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to list KWOK clusters: %w (stderr: %s)", err, errBuf.String())
	}

	lines := strings.Split(strings.TrimSpace(outBuf.String()), "\n")

	var clusters []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			clusters = append(clusters, trimmed)
		}
	}

	return clusters, nil
}
