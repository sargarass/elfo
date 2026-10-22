#![allow(missing_docs)]
#![cfg(all(feature = "network", feature = "turmoil07"))]

use std::time::Duration;

use toml::toml;

use elfo::{
    Topology,
    errors::RequestError,
    messages::Ping,
    prelude::*,
    routers::{MapRouter, Outcome},
    topology,
};
use elfo_test::extract_request;

mod common;

#[test]
fn with_sender() {
    elfo::set_protocol!("remote_requests::with_sender");

    #[message(part)]
    #[derive(Copy, PartialEq, Eq, Hash, derive_more::Display)]
    enum ActorKind {
        A,
        B,
    }

    #[message(ret = ActorKind)]
    struct Query;

    #[message(ret = ActorKind)]
    struct WhoAmI;

    #[message(ret = ())]
    struct RunTest;

    async fn responder(mut ctx: Context<(), ActorKind>) {
        while let Some(envelope) = ctx.recv().await {
            msg!(match envelope {
                (Query, token) => match ctx.key() {
                    ActorKind::A => ctx.respond(token, ActorKind::A),
                    ActorKind::B => drop(token),
                },
                (WhoAmI, token) => ctx.respond(token, *ctx.key()),
                _ => unreachable!(),
            });
        }
    }

    async fn requester(mut ctx: Context) {
        let (RunTest, done) = extract_request(ctx.recv().await.unwrap());

        while ctx.request(Ping::default()).resolve().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let responses = ctx.request(Query).all().resolve().with_sender().await;
        let mut results = Vec::new();
        for (response, sender) in responses {
            assert!(sender.is_remote());
            let key = ctx.request_to(sender, WhoAmI).resolve().await.unwrap();
            results.push((key, response));
        }
        results.sort_by_key(|(_, response)| response.is_err());
        assert!(matches!(
            results.as_slice(),
            [
                (ActorKind::A, Ok(ActorKind::A)),
                (ActorKind::B, Err(RequestError::Ignored)),
            ]
        ));

        turmoil::partition("client", "server");
        let (response, sender) = ctx.request(Query).resolve().with_sender().await;
        assert!(matches!(response, Err(RequestError::Failed)));
        assert!(sender.is_null());

        ctx.respond(done, ());
    }

    common::setup_logger();
    let mut sim = turmoil::Builder::new()
        .enable_tokio_io()
        .tick_duration(Duration::from_millis(100))
        .build();

    sim.host("server", || async {
        let topology = Topology::empty();
        let configurers = topology.local("system.configurers").entrypoint();
        let network = topology.local("system.network");
        let responders = topology.local("responders");
        network.mount(elfo::batteries::network::new(&topology));
        configurers.mount(elfo::batteries::configurer::fixture(
            &topology,
            toml! {
                [system.network]
                listen = ["turmoil07://0.0.0.0"]
                ping_interval = "100ms"
                idle_timeout = "500ms"
            },
        ));
        responders.mount(
            ActorGroup::new()
                .router(MapRouter::new(|envelope| {
                    msg!(match envelope {
                        Ping => Outcome::Unicast(ActorKind::A),
                        Query => Outcome::Multicast(vec![ActorKind::A, ActorKind::B]),
                        _ => Outcome::Default,
                    })
                }))
                .exec(responder),
        );
        Ok(elfo::init::try_start(topology).await?)
    });

    sim.client("client", async {
        let topology = Topology::empty();
        let configurers = topology.local("system.configurers").entrypoint();
        let network = topology.local("system.network");
        let requesters = topology.local("requesters");
        let requester_addr = requesters.addr();
        let responders = topology.remote("responders");
        requesters.route_to(&responders, |_, _| topology::Outcome::Broadcast);
        network.mount(elfo::batteries::network::new(&topology));
        configurers.mount(elfo::batteries::configurer::fixture(
            &topology,
            toml! {
                [system.network]
                discovery.predefined = ["turmoil07://server"]
                discovery.attempt_interval = "0ms"
                ping_interval = "100ms"
                idle_timeout = "500ms"
            },
        ));
        requesters.mount(ActorGroup::new().exec(requester));
        Ok(elfo::_priv::do_start(topology, false, |ctx, _| async move {
            ctx.request_to(requester_addr, RunTest)
                .resolve()
                .await
                .expect("requester failed to complete the test");
        })
        .await?)
    });
    sim.run().unwrap();
}
