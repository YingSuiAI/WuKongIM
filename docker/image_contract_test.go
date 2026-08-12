package docker_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProductionImageContainsOnlyWuKongIMBinary(t *testing.T) {
	dockerfile := readRootDockerfile(t)

	if got := strings.Count(dockerfile, "go build -o /out/wukongim ./cmd/wukongim"); got != 1 {
		t.Fatalf("production binary build count = %d, want 1", got)
	}
	if got := strings.Count(dockerfile, "COPY --from=server-builder /out/wukongim /usr/local/bin/wukongim"); got != 1 {
		t.Fatalf("production binary copy count = %d, want 1", got)
	}
	if !strings.HasSuffix(strings.TrimSpace(dockerfile), "FROM runtime-base AS runtime") {
		t.Fatal("Dockerfile default target must be the production runtime")
	}
	if !strings.Contains(dockerfile, "EXPOSE 5001 5100 5200 5301 7000\n") {
		t.Fatal("production runtime ports drifted")
	}
	devToolsStart := strings.Index(dockerfile, "FROM runtime-base AS dev-tools")
	runtimeStart := strings.LastIndex(dockerfile, "FROM runtime-base AS runtime")
	if devToolsStart < 0 || runtimeStart <= devToolsStart {
		t.Fatal("Dockerfile must isolate development tools before the final runtime target")
	}
	devTools := dockerfile[devToolsStart:runtimeStart]
	if !strings.Contains(devTools, "EXPOSE 19092") {
		t.Error("development tools target must expose its analysis endpoint")
	}
	for _, tool := range []string{"wkbench", "wkanalysis", "wkcloudsim"} {
		want := "COPY --from=tools-builder /out/" + tool + " /usr/local/bin/" + tool
		if !strings.Contains(devTools, want) {
			t.Errorf("development tools target missing %q", want)
		}
	}
}

func TestProductionImageDeclaresOCIProvenanceLabels(t *testing.T) {
	dockerfile := readRootDockerfile(t)

	for _, name := range []string{"OCI_CREATED", "OCI_REVISION", "OCI_SOURCE", "OCI_VERSION"} {
		if !regexp.MustCompile(`(?m)^ARG ` + name + `$`).MatchString(dockerfile) {
			t.Errorf("Dockerfile missing exact ARG %s", name)
		}
	}
	wants := []string{
		`org.opencontainers.image.title="wukongim"`,
		`org.opencontainers.image.description="WuKongIM messaging server"`,
		`org.opencontainers.image.source="${OCI_SOURCE}"`,
		`org.opencontainers.image.revision="${OCI_REVISION}"`,
		`org.opencontainers.image.created="${OCI_CREATED}"`,
		`org.opencontainers.image.version="${OCI_VERSION}"`,
		`org.opencontainers.image.licenses="Apache-2.0"`,
	}
	for _, want := range wants {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("Dockerfile missing OCI label %s", want)
		}
	}
}

func TestDevelopmentComposeUsesExplicitToolsTarget(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join(dockerRepoRoot(t), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	var document struct {
		Services map[string]struct {
			Image string `yaml:"image"`
			Build struct {
				Target string `yaml:"target"`
			} `yaml:"build"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(compose, &document); err != nil {
		t.Fatalf("decode docker-compose.yml: %v", err)
	}
	for _, service := range []string{"wk-sim", "wk-analysis"} {
		got := document.Services[service]
		if got.Image != "wukongim-dev-tools:local" || got.Build.Target != "dev-tools" {
			t.Errorf("%s image/target = %q/%q, want wukongim-dev-tools:local/dev-tools", service, got.Image, got.Build.Target)
		}
	}
}

func readRootDockerfile(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dockerRepoRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatalf("read root Dockerfile: %v", err)
	}
	return string(data)
}
