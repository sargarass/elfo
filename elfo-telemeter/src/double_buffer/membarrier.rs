const CMD_PRIVATE_EXPEDITED: libc::c_long = 1 << 3;
const CMD_REGISTER_PRIVATE_EXPEDITED: libc::c_long = 1 << 4;

pub(super) fn register() -> bool {
    call(CMD_REGISTER_PRIVATE_EXPEDITED) == 0
}

pub(super) fn barrier() -> bool {
    call(CMD_PRIVATE_EXPEDITED) == 0
}

fn call(cmd: libc::c_long) -> libc::c_long {
    // SAFETY: the syscall number and arguments match the membarrier ABI.
    unsafe {
        libc::syscall(
            libc::SYS_membarrier,
            cmd,
            0 as libc::c_long,
            0 as libc::c_long,
        )
    }
}
