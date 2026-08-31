// Source for testdata/fsprobe.wasm — NOT built by `go test`; the compiled
// module is committed and read via go:embed so the test suite is hermetic
// and depends on no wasm toolchain. Regenerate with:
//
//   rustup target add wasm32-wasip1
//   rustc --target wasm32-wasip1 -C opt-level=z -C panic=abort \
//       -C strip=symbols -C lto=yes -o fsprobe.wasm fsprobe.rs
//
// fsprobe.wasm takes a path as its first argument and tries to read it as a
// WASI guest path. It exits 0 (and prints "OK:<contents>") if the read
// succeeds, or exits 1 (and prints "ERR") if it fails — used to drive the
// grants-enforcement property: a task with no declared filesystem access
// can't open any path, and a task with a declared root can reach under it
// but nothing outside it. The exit code (surfaced by wazero as
// sys.ExitError, independent of whether stdout capture is wired) is the
// primary assertion signal, not the printed text.
fn main() {
    let path = std::env::args().nth(1).unwrap_or_default();
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            println!("OK:{}", contents);
            std::process::exit(0);
        }
        Err(_) => {
            println!("ERR");
            std::process::exit(1);
        }
    }
}
