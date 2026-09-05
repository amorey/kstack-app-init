//go:build ignore

// Coverage gate for the sidecar. Runs the suite twice — untagged and with
// `-tags debug`, the only build that compiles the environment overrides — merges
// the two profiles, drops generated files, and fails below the threshold in
// scripts/coverage-threshold.
//
//	go run scripts/coverage.go          # check against the threshold
//	go run scripts/coverage.go -report  # also list every file with a gap
//
// The build tag keeps this file out of `go build ./...` and out of the profile
// it measures; `go run` takes the path directly.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Generated files nobody hand-writes. graph/schema.resolvers.go carries a
// "Code generated" header too but its bodies are ours, so the list is explicit
// rather than derived from the header.
var generated = []string{
	"graph/generated.go",
	"graph/model/models_gen.go",
	"grpc/authpb/",
	"grpc/pokepb/",
}

const modulePath = "github.com/kubetail-org/kstack-app/sidecar/"

func main() {
	report := flag.Bool("report", false, "list every file with uncovered statements")
	flag.Parse()

	// One block per source range. A block is covered if any run reached it:
	// -tags debug compiles code the untagged run cannot see, and vice versa.
	covered := map[string]bool{}
	stmts := map[string]int{}
	for _, tags := range []string{"", "debug"} {
		profile, err := runSuite(tags)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := mergeProfile(profile, stmts, covered); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	total, hit := 0, 0
	missing := map[string]int{}
	fileStmts := map[string]int{}
	for block, n := range stmts {
		if n == 0 {
			continue
		}
		file := block[:strings.Index(block, ":")]
		total += n
		fileStmts[file] += n
		if covered[block] {
			hit += n
		} else {
			missing[file] += n
		}
	}
	if total == 0 {
		fmt.Fprintln(os.Stderr, "coverage: no statements measured")
		os.Exit(1)
	}

	pct := 100 * float64(hit) / float64(total)
	if *report && len(missing) > 0 {
		files := make([]string, 0, len(missing))
		for f := range missing {
			files = append(files, f)
		}
		sort.Slice(files, func(i, j int) bool { return missing[files[i]] > missing[files[j]] })
		for _, f := range files {
			fmt.Printf("%4d uncovered  %5.1f%%  %s\n", missing[f], 100*float64(fileStmts[f]-missing[f])/float64(fileStmts[f]), f)
		}
	}

	want, err := threshold()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("coverage: %.2f%% of %d statements (threshold %.2f%%)\n", pct, total, want)
	if pct+1e-9 < want {
		fmt.Fprintf(os.Stderr, "coverage below threshold; add tests or lower scripts/coverage-threshold deliberately\n")
		os.Exit(1)
	}
}

// runSuite runs the whole module with cross-package coverage and returns the
// profile path. -coverpkg=./... is what makes a helper exercised only from
// another package's test count.
func runSuite(tags string) (string, error) {
	out, err := os.CreateTemp("", "sidecar-cover-*.out")
	if err != nil {
		return "", err
	}
	out.Close()
	args := []string{"test", "./...", "-coverpkg=./...", "-covermode=atomic", "-coverprofile=" + out.Name()}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	cmd := exec.Command("go", args...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go test (tags=%q): %w", tags, err)
	}
	return out.Name(), nil
}

// mergeProfile folds one profile into the running union. With -coverpkg every
// package's test binary reports every block, so the same block arrives many
// times and only the OR of the counts is meaningful.
func mergeProfile(path string, stmts map[string]int, covered map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	defer os.Remove(path)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		// "<file>:<range> <numStmts> <count>"
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return fmt.Errorf("%s: malformed profile line %q", path, line)
		}
		block := strings.TrimPrefix(fields[0], modulePath)
		if isGenerated(block) {
			continue
		}
		n, err := strconv.Atoi(fields[1])
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		stmts[block] = n
		covered[block] = covered[block] || count > 0
	}
	return sc.Err()
}

func isGenerated(block string) bool {
	for _, g := range generated {
		if strings.HasPrefix(block, g) {
			return true
		}
	}
	return false
}

func threshold() (float64, error) {
	raw, err := os.ReadFile(filepath.Join("scripts", "coverage-threshold"))
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
}
