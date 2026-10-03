cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        use tcmalloc_better::TCMalloc;

        mod messaging;

        #[global_allocator]
        static ALLOCATOR: TCMalloc = TCMalloc;

        criterion::criterion_main!(messaging::cases);
    } else {
        fn main() {
            eprintln!("messaging_tc requires Linux");
            std::process::exit(1);
        }
    }
}
