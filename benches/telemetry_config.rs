#[derive(Clone, Copy)]
pub(crate) enum Telemetry {
    Off,
    Group,
    Actor,
    Both,
}

impl Telemetry {
    pub(crate) fn from_env(default: Self) -> Self {
        match std::env::var("ELFO_BENCH_TELEMETRY") {
            Ok(mode) => match mode.as_str() {
                "off" => Self::Off,
                "group" => Self::Group,
                "actor" => Self::Actor,
                "both" => Self::Both,
                _ => panic!("unknown ELFO_BENCH_TELEMETRY: {mode}"),
            },
            Err(std::env::VarError::NotPresent) => default,
            Err(err) => panic!("invalid ELFO_BENCH_TELEMETRY: {err}"),
        }
    }

    pub(crate) fn config(self) -> toml::Value {
        let per_actor_group = matches!(self, Self::Group | Self::Both);
        let per_actor_key = matches!(self, Self::Actor | Self::Both);

        toml::toml! {
            per_actor_group = per_actor_group
            per_actor_key = per_actor_key
        }
        .into()
    }
}
