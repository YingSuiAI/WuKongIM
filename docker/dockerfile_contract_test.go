package docker_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultProductionImageContainsOnlyServerBinaryAndProvenance(t *testing.T) {
	dockerfilePath := filepath.Join(dockerRepoRoot(t), "Dockerfile")
	data, err := os.ReadFile(dockerfilePath)
	if err != nil {
		t.Fatalf("read %s: %v", dockerfilePath, err)
	}
	dockerfile := string(data)

	const productionStage = "FROM ${RUNTIME_IMAGE} AS production"
	stageStart := strings.LastIndex(dockerfile, productionStage)
	if stageStart == -1 {
		t.Fatalf("Dockerfile missing final %q stage", productionStage)
	}
	production := dockerfile[stageStart:]
	if strings.Count(production, "\nFROM ") != 0 {
		t.Fatal("production must remain the final stage so the default build is the production image")
	}

	const serverCopy = "COPY --from=builder /out/wukongim /usr/local/bin/wukongim"
	if strings.Count(production, serverCopy) != 1 {
		t.Fatalf("production stage must copy exactly one server binary with %q", serverCopy)
	}
	for _, tool := range []string{"wkbench", "wkanalysis", "wkcloudsim"} {
		if strings.Contains(production, tool) {
			t.Errorf("production stage must not contain development tool %q", tool)
		}
		if strings.Contains(dockerfile[:stageStart], "go build -o /out/"+tool) ||
			strings.Contains(dockerfile[:stageStart], "go build -trimpath -o /out/"+tool) {
			t.Errorf("default builder must not build development tool %q", tool)
		}
	}

	labels := map[string]string{
		"source":   "OCI_SOURCE",
		"revision": "OCI_REVISION",
		"version":  "OCI_VERSION",
		"created":  "OCI_CREATED",
		"licenses": "OCI_LICENSES",
	}
	for label, buildArg := range labels {
		if !strings.Contains(production, "ARG "+buildArg) {
			t.Errorf("production stage missing build argument %s", buildArg)
		}
		want := "org.opencontainers.image." + label + "=\"${" + buildArg + "}\""
		if !strings.Contains(production, want) {
			t.Errorf("production stage missing OCI label %q", want)
		}
	}

	const entrypoint = `ENTRYPOINT ["/usr/local/bin/wukongim", "-config", "/etc/wukongim/wukongim.toml"]`
	if !strings.Contains(production, entrypoint) {
		t.Fatalf("production stage missing server entrypoint %q", entrypoint)
	}
}
