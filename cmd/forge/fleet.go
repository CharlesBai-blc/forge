package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
)

func runWorkers(args []string, stdout io.Writer) error {
	st, err := parseFleetStore("workers", args, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	workers, err := st.ListWorkers(context.Background())
	if err != nil {
		return err
	}
	for _, w := range workers {
		if _, err := fmt.Fprintf(stdout, "%s\t%s\t%s\n", w.ID, w.Name, w.State); err != nil {
			return err
		}
	}
	return nil
}

func runCordon(args []string, stdout io.Writer) error {
	return runWorkerState("cordon", args, stdout, job.WorkerCordoned)
}

func runUncordon(args []string, stdout io.Writer) error {
	return runWorkerState("uncordon", args, stdout, job.WorkerActive)
}

func runDrain(args []string, stdout io.Writer) error {
	return runWorkerState("drain", args, stdout, job.WorkerDraining)
}

func runRemove(args []string, stdout io.Writer) error {
	st, id, err := parseFleetWorker("remove", args)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.RemoveWorker(context.Background(), id); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, id)
	return err
}

func runWorkerState(name string, args []string, stdout io.Writer, to job.WorkerState) error {
	st, id, err := parseFleetWorker(name, args)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.TransitionWorker(context.Background(), id, to); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, id)
	return err
}

func parseFleetStore(name string, args []string, wantArgs int) (*store.Store, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	dataDir := fs.String("data-dir", envOr("FORGE_DATA_DIR", "./data"), "data directory")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() != wantArgs {
		if wantArgs == 1 {
			return nil, fmt.Errorf("forge %s: worker id required", name)
		}
		return nil, fmt.Errorf("forge %s: unexpected arguments", name)
	}
	return openCLIStore(*dataDir)
}

func parseFleetWorker(name string, args []string) (*store.Store, string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	dataDir := fs.String("data-dir", envOr("FORGE_DATA_DIR", "./data"), "data directory")
	if err := fs.Parse(args); err != nil {
		return nil, "", err
	}
	if fs.NArg() != 1 {
		return nil, "", fmt.Errorf("forge %s: worker id required", name)
	}
	st, err := openCLIStore(*dataDir)
	if err != nil {
		return nil, "", err
	}
	return st, fs.Arg(0), nil
}
