fn main() {
    println!("cargo:rerun-if-changed=vendor/flags2env/native/parser.c");
    println!("cargo:rerun-if-changed=vendor/flags2env/native/parser.h");

    cc::Build::new()
        .file("vendor/flags2env/native/parser.c")
        .include("vendor/flags2env/native")
        .flag_if_supported("-std=c99")
        .warnings(false)
        .compile("flags2env");
}
