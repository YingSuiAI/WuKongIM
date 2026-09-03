package scripts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContainerImageBuildReusesGoCaches(t *testing.T) {
	dockerfile, err := os.ReadFile(filepath.Join(repoRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	source := string(dockerfile)

	if got := strings.Count(source, "id=wukongim-go-mod,target=/go/pkg/mod,sharing=locked"); got != 3 {
		t.Fatalf("Go module cache mount count = %d, want 3", got)
	}
	if got := strings.Count(source, "id=wukongim-go-build,target=/root/.cache/go-build,sharing=locked"); got != 2 {
		t.Fatalf("Go build cache mount count = %d, want 2", got)
	}
	if strings.Contains(source, "RUN go mod download") {
		t.Fatal("go mod download bypasses the persistent BuildKit cache mount")
	}
}
