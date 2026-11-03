#!/usr/bin/env python3
import importlib.util, sys, yaml
import v17_turboflow_econ as base

spec = importlib.util.spec_from_file_location("v17_exact_private", "work/v17_exact_private.py")
exact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exact)

def load_exact(_private_root):
    return exact.SignalEngine, yaml.safe_load(exact.CONFIG_YAML)

base.load_engine = load_exact

if __name__ == "__main__":
    base.main()
