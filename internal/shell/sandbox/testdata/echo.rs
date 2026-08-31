// Source for testdata/echo.wasm — NOT built by `go test`; the compiled
// module is committed and read via go:embed so the test suite is hermetic
// and depends on no wasm toolchain. Regenerate with:
//
//   rustup target add wasm32-wasip1
//   rustc --target wasm32-wasip1 -C opt-level=z -C panic=abort \
//       -C strip=symbols -C lto=yes -o echo.wasm echo.rs
//
// echo.wasm ignores its arguments/stdin and writes a fixed line to stdout,
// then exits 0 — it exercises the "signed module runs to completion and its
// captured output becomes the task result" acceptance criterion.
fn main() {
    println!("sandbox-ok");
}
