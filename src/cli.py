"""CLI for ifc2lbd"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ifc2lbd.convert import ifc_to_lbd_ttl, ifc_to_lbd_trig


def log(message: str, verbose: bool = True):
    """Log message with timestamp if verbose mode is enabled."""
    if verbose:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="ifc2lbd",
        description="Convert IFC files to LBD (Linked Building Data) format",
        epilog="Example: ifc2lbd --inputs file1.ifc file2.ifc --outputs file1.trig file2.trig --verbose"
    )
    
    parser.add_argument(
        "--inputs", "-i",
        nargs="+",
        required=True,
        metavar="IFC",
        help="Input IFC file path(s) - at least one required"
    )
    
    parser.add_argument(
        "--outputs", "-o",
        nargs="+",
        required=True,
        metavar="OUTPUT",
        help="Output file path(s) - must match number of inputs"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output with timestamps"
    )
    
    parser.add_argument(
        "--stream", "-s",
        action="store_true",
        help="Stream IFC files instead of loading to memory (uses 'stream2' function)." \
        "Currently only available in ifcopenshell:ifcopenshell conda channel (Alpha version). To use it make sure your IfcOpenShell version has 'stream2' function, run: pixi run -e experimental-conda"
    )
    
    parser.add_argument(
        "--profile", "-p",
        action="store_true",
        help="Enable cProfile profiling and save detailed performance stats"
    )
    
    parser.add_argument(
        "--benchmark", "-b",
        action="store_true",
        help="Enable benchmark mode - returns detailed metrics for performance analysis"
    )
    
    parser.add_argument(
        "--converter", "-c",
        choices=["mini_ifcowl", "mini_ifcowl_complete", "mini_ifcowl_complete2"], 
        default="mini_ifcowl_complete",
        help="Choose conversion method: 'mini_ifcowl' (default, simplified), 'ifcowl' (with declared types), 'mini_ifcowl_optimized' (optimized streaming(read about it in docs))"
    )
    
    parser.add_argument(
        "--single-pass",
        action="store_true",
        help="Use single-pass mode (skip entity type mapping). Mind, only works with mini_ifcowl_optimized converter for now. References will be inst:Entity_ID instead of inst:IfcType_ID"
    )
    
    args = parser.parse_args()
    
    # Validate that inputs and outputs have the same count
    if len(args.inputs) != len(args.outputs):
        print(f"Error: Number of inputs ({len(args.inputs)}) must match number of outputs ({len(args.outputs)})", 
              file=sys.stderr)
        sys.exit(1)
    
    log("Starting IFC to LBD conversion", args.verbose)
    log(f"Processing {len(args.inputs)} file(s)", args.verbose)
    log(f"Mode: {'Streaming' if args.stream else 'Load to memory'}", args.verbose)
    log(f"Converter: {args.converter}", args.verbose)
    
    # Validate single-pass flag
    if args.single_pass and args.converter != "mini_ifcowl_optimized":
        print("Error: --single-pass only works with --converter mini_ifcowl_optimized", file=sys.stderr)
        sys.exit(1)
    
    if args.single_pass:
        log("Single-pass mode: enabled (generic entity references)", args.verbose)
    
    # Validate all input files exist
    for input_file in args.inputs:
        input_path = Path(input_file)
        if not input_path.exists():
            print(f"Error: Input file '{input_file}' does not exist", file=sys.stderr)
            sys.exit(1)
        
        if not input_path.suffix.lower() == '.ifc':
            log(f"Warning: '{input_file}' does not have .ifc extension", args.verbose)
    
    # Determine output format based on number of files
    is_multiple = len(args.inputs) > 1
    expected_format = ".trig" if is_multiple else ".ttl"
    
    # Validate output formats
    for output_file in args.outputs:
        output_path = Path(output_file)
        if is_multiple and output_path.suffix.lower() != '.trig':
            print(f"Error: Multiple inputs require TRIG output format, but got '{output_file}'", 
                  file=sys.stderr)
            sys.exit(1)
    
    # Perform conversions
    success_count = 0
    # Collect metrics if benchmark mode enabled
    all_metrics = []
    
    for idx, (input_file, output_file) in enumerate(zip(args.inputs, args.outputs), 1):
        try:
            log(f"[{idx}/{len(args.inputs)}] Converting '{input_file}' -> '{output_file}'", args.verbose)
            
            if is_multiple:
                # Use TRIG format for multiple files
                metrics = ifc_to_lbd_trig(input_file, output_file, stream=args.stream, verbose=args.verbose, profile=args.profile, converter=args.converter, return_metrics=args.benchmark)
            else:
                # Use TTL format for single file
                metrics = ifc_to_lbd_ttl(input_file, output_file, stream=args.stream, verbose=args.verbose, profile=args.profile, converter=args.converter, return_metrics=args.benchmark, single_pass=args.single_pass)
            
            if args.benchmark and metrics:
                all_metrics.append(metrics)
            
            log(f"[{idx}/{len(args.inputs)}] Completed successfully", args.verbose)
            success_count += 1
            
        except Exception as e:
            print(f"Error converting '{input_file}': {e}", file=sys.stderr)
            if args.verbose:
                import traceback
                traceback.print_exc()
    
    # Print benchmark results if requested
    if args.benchmark and all_metrics:
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)
        for metrics in all_metrics:
            print(f"\nFile: {metrics.get('input_file')}")
            print(f"  Entities processed: {metrics.get('entities_processed', 'N/A')}")
            print(f"  Triples written: {metrics.get('triples_written', 'N/A')}")
            print(f"  Load time: {metrics.get('load_time', 0):.3f}s")
            print(f"  Write time: {metrics.get('write_time', 0):.3f}s")
            print(f"  Total time: {metrics.get('total_time', 0):.3f}s")
            if metrics.get('triples_written') and metrics.get('total_time'):
                throughput = metrics['triples_written'] / metrics['total_time']
                print(f"  Throughput: {throughput:.0f} triples/second")
        print("=" * 80)
    
    # Summary
    log("=" * 50, args.verbose)
    log(f"Conversion complete: {success_count}/{len(args.inputs)} successful", args.verbose)
    
    if success_count < len(args.inputs):
        sys.exit(1)


if __name__ == "__main__":
    main()