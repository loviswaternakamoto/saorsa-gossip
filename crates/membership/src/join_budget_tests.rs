//! Tests for the step-by-step JOIN API ([`HyParViewMembership::join_dial`],
//! [`HyParViewMembership::join_send`], [`JoinLedger`]) and for
//! [`Membership::join`], which now runs on it.
//!
//! The differential tests run a verbatim copy of the 0.5.87 `join` loop
//! ([`reference_join_0_5_87`], with its `add_active`,
//! [`reference_add_active_0_5_87`]) and the current [`Membership::join`]
//! against the same scripted transport, and compare the dials, the JOIN
//! frames, the stored addresses, the active view, the SWIM state, the result
//! and the backoff between dials. The tests use paused time (or a plain
//! thread) and a scripted transport: no sockets.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::time::Instant as TokioInstant;

/// One scripted `dial_bootstrap`.
#[derive(Debug, Clone, Copy)]
struct Dial {
    /// How long the dial takes.
    took: Duration,
    /// The answering peer, or `None` for a failed dial.
    answer: Option<PeerId>,
}

fn answers(peer: PeerId) -> Dial {
    Dial {
        took: Duration::ZERO,
        answer: Some(peer),
    }
}

fn fails() -> Dial {
    Dial {
        took: Duration::ZERO,
        answer: None,
    }
}

#[derive(Default)]
struct Script {
    /// Per address: the dial outcomes in order; the last one repeats.
    dials: HashMap<SocketAddr, Vec<Dial>>,
    /// Per peer: JOIN send outcomes in order (`true` = sent); the last one
    /// repeats. A peer without a plan always succeeds.
    join_sends: HashMap<PeerId, Vec<bool>>,
    /// How long a JOIN send takes.
    join_send_took: Duration,
}

#[derive(Default)]
struct Recorded {
    dials: Vec<(SocketAddr, TokioInstant)>,
    /// JOIN frames that the transport accepted: (peer, stream, bytes).
    join_frames: Vec<(PeerId, GossipStreamType, Bytes)>,
    /// JOIN send calls, accepted or not.
    join_send_calls: usize,
    /// The target of each JOIN send call.
    join_send_targets: Vec<PeerId>,
}

/// A [`GossipTransport`] with scripted dials and JOIN sends. It records
/// every `dial_bootstrap` and every JOIN frame. Other frames (SWIM probes,
/// shuffles from the background tasks) are accepted and not recorded.
struct ScriptedTransport {
    local: PeerId,
    script: Script,
    recorded: Mutex<Recorded>,
}

impl ScriptedTransport {
    fn new(local: PeerId, script: Script) -> Arc<Self> {
        Arc::new(Self {
            local,
            script,
            recorded: Mutex::new(Recorded::default()),
        })
    }

    fn dial_addrs(&self) -> Vec<SocketAddr> {
        self.recorded
            .lock()
            .unwrap()
            .dials
            .iter()
            .map(|(addr, _)| *addr)
            .collect()
    }

    fn dial_times(&self) -> Vec<TokioInstant> {
        self.recorded
            .lock()
            .unwrap()
            .dials
            .iter()
            .map(|(_, at)| *at)
            .collect()
    }

    fn join_frames(&self) -> Vec<(PeerId, GossipStreamType, Bytes)> {
        self.recorded.lock().unwrap().join_frames.clone()
    }

    fn join_send_calls(&self) -> usize {
        self.recorded.lock().unwrap().join_send_calls
    }
}

fn is_join_frame(data: &[u8]) -> bool {
    matches!(
        postcard::from_bytes::<MembershipProtocolMessage>(data),
        Ok(MembershipProtocolMessage::HyParView(
            HyParViewMessage::Join { .. }
        ))
    )
}

#[async_trait::async_trait]
impl GossipTransport for ScriptedTransport {
    async fn dial(&self, _peer: PeerId, _addr: SocketAddr) -> Result<()> {
        Ok(())
    }

    async fn dial_bootstrap(&self, addr: SocketAddr) -> Result<PeerId> {
        let dial = {
            let mut recorded = self.recorded.lock().unwrap();
            let earlier = recorded.dials.iter().filter(|(a, _)| *a == addr).count();
            recorded.dials.push((addr, TokioInstant::now()));
            self.script
                .dials
                .get(&addr)
                .and_then(|plan| plan.get(earlier.min(plan.len().saturating_sub(1))))
                .copied()
                .unwrap_or_else(fails)
        };
        // No timer for an instant dial, so that a JOIN can run outside a
        // Tokio runtime.
        if !dial.took.is_zero() {
            tokio::time::sleep(dial.took).await;
        }
        dial.answer
            .ok_or_else(|| anyhow!("scripted dial failure to {addr}"))
    }

    async fn listen(&self, _bind: SocketAddr) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn send_to_peer(
        &self,
        peer: PeerId,
        stream_type: GossipStreamType,
        data: Bytes,
    ) -> Result<()> {
        if !is_join_frame(&data) {
            return Ok(());
        }
        let sent = {
            let mut recorded = self.recorded.lock().unwrap();
            let earlier = recorded.join_send_calls_to(peer);
            recorded.join_send_calls += 1;
            recorded.join_send_targets.push(peer);
            self.script
                .join_sends
                .get(&peer)
                .and_then(|plan| plan.get(earlier.min(plan.len().saturating_sub(1))))
                .copied()
                .unwrap_or(true)
        };
        if !self.script.join_send_took.is_zero() {
            tokio::time::sleep(self.script.join_send_took).await;
        }
        if !sent {
            return Err(anyhow!("scripted JOIN send failure to {peer}"));
        }
        self.recorded
            .lock()
            .unwrap()
            .join_frames
            .push((peer, stream_type, data));
        Ok(())
    }

    async fn receive_message(&self) -> Result<(PeerId, GossipStreamType, Bytes)> {
        Err(anyhow!("scripted transport has no receive queue"))
    }

    fn local_peer_id(&self) -> PeerId {
        self.local
    }
}

impl Recorded {
    fn join_send_calls_to(&self, peer: PeerId) -> usize {
        self.join_send_targets
            .iter()
            .filter(|target| **target == peer)
            .count()
    }
}

fn peer(byte: u8) -> PeerId {
    PeerId::new([byte; 32])
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

const LOCAL: u8 = 0xEE;

fn membership(
    script: Script,
) -> (
    HyParViewMembership<ScriptedTransport>,
    Arc<ScriptedTransport>,
) {
    let transport = ScriptedTransport::new(peer(LOCAL), script);
    let membership = HyParViewMembership::new(
        peer(LOCAL),
        MembershipConfig::default(),
        Arc::clone(&transport),
    );
    (membership, transport)
}

/// A verbatim copy of `Membership::join` from saorsa-gossip-membership
/// 0.5.87 (`src/lib.rs:1630-1716` of that release), the reference for the
/// differential tests.
async fn reference_join_0_5_87<T: GossipTransport + 'static>(
    this: &HyParViewMembership<T>,
    seeds: Vec<String>,
) -> Result<()> {
    use std::net::SocketAddr;

    if seeds.is_empty() {
        debug!("JOIN: No seeds provided, operating as bootstrap node");
        return Ok(());
    }

    // Exponential backoff parameters
    const INITIAL_DELAY_MS: u64 = 100;
    const MAX_DELAY_MS: u64 = 30_000;
    const MAX_RETRIES: usize = 10;

    for seed in seeds {
        // Parse the seed address
        let addr: SocketAddr = match seed.parse() {
            Ok(a) => a,
            Err(e) => {
                warn!(seed = %seed, error = %e, "JOIN: Invalid seed address");
                continue;
            }
        };

        let mut delay_ms = INITIAL_DELAY_MS;
        let mut connected = false;

        for attempt in 0..MAX_RETRIES {
            debug!(
                seed = %seed,
                attempt = attempt + 1,
                "JOIN: Connecting to bootstrap node"
            );

            // Connect via transport and get peer ID
            match this.transport.dial_bootstrap(addr).await {
                Ok(seed_peer_id) => {
                    debug!(
                        seed = %seed,
                        peer_id = %seed_peer_id,
                        "JOIN: Connected to bootstrap"
                    );

                    // Store peer address
                    this.store_peer_addr(seed_peer_id, addr).await;

                    // Send JOIN message
                    let join_msg = HyParViewMessage::Join {
                        sender: this.local_peer_id,
                        ttl: ACTIVE_RANDOM_WALK_LENGTH,
                    };

                    if let Err(e) = this.send_hyparview_message(seed_peer_id, &join_msg).await {
                        warn!(error = %e, "JOIN: Failed to send JOIN message");
                        continue;
                    }

                    // Add seed to active view immediately (optimistic)
                    reference_add_active_0_5_87(this, seed_peer_id).await?;
                    connected = true;
                    break;
                }
                Err(e) => {
                    warn!(
                        seed = %seed,
                        attempt = attempt + 1,
                        error = %e,
                        delay_ms = delay_ms,
                        "JOIN: Connection failed, will retry"
                    );

                    // Exponential backoff with jitter
                    let jitter =
                        (rand::random::<u64>() % (delay_ms / 4)).saturating_sub(delay_ms / 8);
                    tokio::time::sleep(Duration::from_millis(delay_ms + jitter)).await;
                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                }
            }
        }

        if connected {
            // Successfully joined via this seed
            debug!(seed = %seed, "JOIN: Successfully joined network");
            return Ok(());
        }
    }

    Err(anyhow!("JOIN: Failed to connect to any seed nodes"))
}

/// A verbatim copy of `Membership::add_active` from
/// saorsa-gossip-membership 0.5.87 (`src/lib.rs:1741-1764` of that
/// release), on `this` instead of `self`.
async fn reference_add_active_0_5_87<T: GossipTransport + 'static>(
    this: &HyParViewMembership<T>,
    peer: PeerId,
) -> Result<()> {
    let mut active = this.active.write().await;

    // If active view is full, demote one peer to passive
    if active.len() >= this.config.active_degree {
        if let Some(&to_demote) = active.iter().next() {
            active.remove(&to_demote);
            // Move to passive view
            let mut passive = this.passive.write().await;
            if passive.len() < this.config.max_passive_degree {
                passive.insert(to_demote);
                debug!(peer_id = %to_demote, "Demoted to passive (active view full)");
            }
        }
    }

    active.insert(peer);
    drop(active); // Release lock before async call

    this.swim.mark_alive(peer).await;
    debug!(peer_id = %peer, "Added to active view");

    Ok(())
}

/// Everything a JOIN run leaves behind that the tests compare.
#[derive(Debug, PartialEq)]
struct Outcome {
    result: std::result::Result<(), String>,
    dials: Vec<SocketAddr>,
    join_frames: Vec<(PeerId, GossipStreamType, Bytes)>,
    join_send_calls: usize,
    stored_addrs: Vec<(PeerId, Option<SocketAddr>)>,
    active: Vec<PeerId>,
    swim_alive: Vec<(PeerId, Option<PeerState>)>,
}

async fn outcome(
    membership: &HyParViewMembership<ScriptedTransport>,
    transport: &ScriptedTransport,
    result: Result<()>,
    peers: &[PeerId],
) -> Outcome {
    let mut stored_addrs = Vec::new();
    let mut swim_alive = Vec::new();
    for peer in peers {
        stored_addrs.push((*peer, membership.get_peer_addr(peer).await));
        swim_alive.push((*peer, membership.swim().get_state(peer).await));
    }
    let mut active = membership.active_view();
    active.sort_by_key(|peer| peer.to_bytes());
    Outcome {
        result: result.map_err(|e| e.to_string()),
        dials: transport.dial_addrs(),
        join_frames: transport.join_frames(),
        join_send_calls: transport.join_send_calls(),
        stored_addrs,
        active,
        swim_alive,
    }
}

/// The base backoff (before jitter) before each dial in `dials` after the
/// first, for the 0.5.87 loop: after a failed dial, that dial's delay; after
/// an answered dial, none. The delay starts at the first backoff for each
/// seed-list entry (a new address, or the same address after its ten
/// attempts) and doubles after each failed dial. `answered[i]` says whether
/// dial `i` answered.
fn expected_gaps(dials: &[SocketAddr], answered: &[bool]) -> Vec<u64> {
    let mut gaps = Vec::new();
    let mut delay = JOIN_INITIAL_DELAY_MS;
    let mut in_entry = 0;
    let mut last_sleep = 0;
    for (i, dial) in dials.iter().enumerate() {
        if i > 0 {
            gaps.push(if answered[i - 1] { 0 } else { last_sleep });
            if *dial != dials[i - 1] || in_entry == JOIN_MAX_ATTEMPTS {
                delay = JOIN_INITIAL_DELAY_MS;
                in_entry = 0;
            }
        }
        in_entry += 1;
        if !answered[i] {
            last_sleep = delay;
            delay = (delay * 2).min(JOIN_MAX_DELAY_MS);
        }
    }
    gaps
}

/// Each gap between dials is the base backoff plus at most 1/8 jitter.
fn assert_gaps(times: &[TokioInstant], expected: &[u64], who: &str) {
    assert_eq!(times.len(), expected.len() + 1, "{who}: dial count");
    for (i, base) in expected.iter().enumerate() {
        let gap = times[i + 1].duration_since(times[i]).as_millis() as u64;
        assert!(
            gap >= *base && gap <= base + base / 8,
            "{who}: gap {i} is {gap} ms, expected {base} ms plus at most 1/8 jitter"
        );
    }
}

struct Scenario {
    name: &'static str,
    seeds: Vec<String>,
    script: fn() -> Script,
    peers: Vec<PeerId>,
}

async fn run_reference(scenario: &Scenario) -> (Outcome, Vec<TokioInstant>) {
    let (membership, transport) = membership((scenario.script)());
    let result = reference_join_0_5_87(&membership, scenario.seeds.clone()).await;
    let outcome = outcome(&membership, &transport, result, &scenario.peers).await;
    (outcome, transport.dial_times())
}

async fn run_join(scenario: &Scenario) -> (Outcome, Vec<TokioInstant>) {
    let (membership, transport) = membership((scenario.script)());
    let result = membership.join(scenario.seeds.clone()).await;
    let outcome = outcome(&membership, &transport, result, &scenario.peers).await;
    (outcome, transport.dial_times())
}

/// The step API, driven the way the old loop runs: seeds in order, one
/// ledger per seed-list entry.
async fn run_steps(scenario: &Scenario) -> (Outcome, Vec<TokioInstant>) {
    let (membership, transport) = membership((scenario.script)());
    let mut result = if scenario.seeds.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("JOIN: Failed to connect to any seed nodes"))
    };
    'seeds: for seed in &scenario.seeds {
        let Ok(seed) = seed.parse::<SocketAddr>() else {
            continue;
        };
        let mut ledger = JoinLedger::new();
        loop {
            match membership.join_dial(&mut ledger, seed).await {
                JoinDial::Answered(peer) => match membership.join_send(seed, peer).await {
                    Ok(JoinSend::Joined) => {
                        result = Ok(());
                        break 'seeds;
                    }
                    Ok(JoinSend::SendFailed) => {}
                    Err(e) => {
                        result = Err(e);
                        break 'seeds;
                    }
                },
                JoinDial::Failed { retry_after, .. } => tokio::time::sleep(retry_after).await,
                JoinDial::NotReady { ready_at } => tokio::time::sleep_until(ready_at).await,
                JoinDial::Exhausted => break,
            }
        }
    }
    let outcome = outcome(&membership, &transport, result, &scenario.peers).await;
    (outcome, transport.dial_times())
}

const A: u16 = 5001;
const B: u16 = 5002;

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "first dial answers, send succeeds",
            seeds: vec![addr(A).to_string()],
            script: || Script {
                dials: HashMap::from([(addr(A), vec![answers(peer(1))])]),
                ..Script::default()
            },
            peers: vec![peer(1)],
        },
        Scenario {
            // Codex finding on x0x bf034a2: nine sends fail, the tenth dial
            // fails. Only a failed dial grows the backoff, so the wait
            // before seed B is the first backoff (100 ms), not 30 s.
            name: "nine failed sends, then a failed dial, then the next seed",
            seeds: vec![addr(A).to_string(), addr(B).to_string()],
            script: || {
                let mut a = vec![answers(peer(1)); 9];
                a.push(fails());
                Script {
                    dials: HashMap::from([(addr(A), a), (addr(B), vec![answers(peer(2))])]),
                    join_sends: HashMap::from([(peer(1), vec![false])]),
                    ..Script::default()
                }
            },
            peers: vec![peer(1), peer(2)],
        },
        Scenario {
            name: "failed dials, an answer with failed sends, failed dials, then joined",
            seeds: vec![addr(A).to_string()],
            script: || Script {
                dials: HashMap::from([(
                    addr(A),
                    vec![
                        fails(),
                        fails(),
                        answers(peer(1)),
                        answers(peer(1)),
                        fails(),
                        answers(peer(1)),
                    ],
                )]),
                join_sends: HashMap::from([(peer(1), vec![false, false, true])]),
                ..Script::default()
            },
            peers: vec![peer(1)],
        },
        Scenario {
            name: "every dial fails on two seeds",
            seeds: vec![addr(A).to_string(), addr(B).to_string()],
            script: || Script::default(),
            peers: vec![],
        },
        Scenario {
            name: "an invalid seed is skipped",
            seeds: vec!["not an address".to_string(), addr(B).to_string()],
            script: || Script {
                dials: HashMap::from([(addr(B), vec![answers(peer(2))])]),
                ..Script::default()
            },
            peers: vec![peer(2)],
        },
        Scenario {
            name: "a repeated seed-list entry gets its own budget",
            seeds: vec![addr(A).to_string(), addr(A).to_string()],
            script: || Script::default(),
            peers: vec![],
        },
        Scenario {
            name: "no seeds",
            seeds: vec![],
            script: || Script::default(),
            peers: vec![],
        },
    ]
}

/// Whether each recorded dial answered, from the scenario's script.
fn answered_flags(scenario: &Scenario, dials: &[SocketAddr]) -> Vec<bool> {
    let script = (scenario.script)();
    let mut seen: HashMap<SocketAddr, usize> = HashMap::new();
    dials
        .iter()
        .map(|addr| {
            let earlier = seen.entry(*addr).or_default();
            let dial = script
                .dials
                .get(addr)
                .and_then(|plan| plan.get((*earlier).min(plan.len().saturating_sub(1))))
                .copied()
                .unwrap_or_else(fails);
            *earlier += 1;
            dial.answer.is_some()
        })
        .collect()
}

/// The differential test: the 0.5.87 loop, the current `join` and the step
/// API make the same dials, send the same JOIN frames, store the same
/// addresses, leave the same active view and SWIM state, return the same
/// result, and wait the same backoff between dials.
#[tokio::test(start_paused = true)]
async fn join_matches_the_0_5_87_loop_frame_for_frame() {
    for scenario in scenarios() {
        let (reference, reference_times) = run_reference(&scenario).await;
        let (join, join_times) = run_join(&scenario).await;
        let (steps, steps_times) = run_steps(&scenario).await;
        assert_eq!(join, reference, "{}: join vs 0.5.87", scenario.name);
        assert_eq!(steps, reference, "{}: step API vs 0.5.87", scenario.name);

        let gaps = if reference.dials.is_empty() {
            Vec::new()
        } else {
            expected_gaps(
                &reference.dials,
                &answered_flags(&scenario, &reference.dials),
            )
        };
        let label = |who: &str| format!("{}: {who}", scenario.name);
        if !reference.dials.is_empty() {
            assert_gaps(&reference_times, &gaps, &label("0.5.87"));
            assert_gaps(&join_times, &gaps, &label("join"));
            assert_gaps(&steps_times, &gaps, &label("steps"));
        }
    }
}

/// The JOIN frame itself: postcard of
/// `MembershipProtocolMessage::HyParView(Join { sender, ttl: 6 })` on the
/// membership stream, sent to the answered peer.
#[tokio::test(start_paused = true)]
async fn join_send_sends_the_join_frame_to_the_answered_peer() {
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(addr(A), vec![answers(peer(1))])]),
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(peer(1))
    );
    assert_eq!(
        membership.join_send(addr(A), peer(1)).await.unwrap(),
        JoinSend::Joined
    );
    let expected = postcard::to_stdvec(&MembershipProtocolMessage::HyParView(
        HyParViewMessage::Join {
            sender: peer(LOCAL),
            ttl: ACTIVE_RANDOM_WALK_LENGTH,
        },
    ))
    .unwrap();
    assert_eq!(
        transport.join_frames(),
        vec![(peer(1), GossipStreamType::Membership, Bytes::from(expected))]
    );
    assert_eq!(membership.get_peer_addr(&peer(1)).await, Some(addr(A)));
    assert_eq!(membership.active_view(), vec![peer(1)]);
    assert_eq!(
        membership.swim().get_state(&peer(1)).await,
        Some(PeerState::Alive)
    );
}

/// One `join_dial` on `seed` once its readiness has come: the caller's
/// obligation to wait `retry_after` (or `ready_at`) between attempts.
async fn dial_when_ready(
    membership: &HyParViewMembership<ScriptedTransport>,
    ledger: &mut JoinLedger,
    seed: SocketAddr,
) -> JoinDial {
    if let Some(ready_at) = ledger.ready_at(seed) {
        tokio::time::sleep_until(ready_at).await;
    }
    membership.join_dial(ledger, seed).await
}

/// One ledger across candidates: each seed keeps its own attempts and
/// backoff while the caller turns between seeds, and a seed that used its
/// attempts stays exhausted (no dial).
#[tokio::test(start_paused = true)]
async fn the_ledger_keeps_each_seed_budget_across_candidates() {
    let (membership, transport) = membership(Script::default());
    let mut ledger = JoinLedger::new();

    for round in 0..JOIN_MAX_ATTEMPTS {
        let a = dial_when_ready(&membership, &mut ledger, addr(A)).await;
        let JoinDial::Failed { exhausted, .. } = a else {
            panic!("A dial {round}: {a:?}");
        };
        assert_eq!(exhausted, round + 1 == JOIN_MAX_ATTEMPTS);
        if round < 3 {
            assert!(matches!(
                dial_when_ready(&membership, &mut ledger, addr(B)).await,
                JoinDial::Failed { .. }
            ));
        }
    }
    assert_eq!(ledger.attempts(addr(A)), JOIN_MAX_ATTEMPTS);
    assert!(ledger.is_exhausted(addr(A)));
    assert_eq!(ledger.attempts(addr(B)), 3);
    assert_eq!(ledger.delay_ms(addr(B)), 800);
    assert_eq!(ledger.delay_ms(addr(A)), JOIN_MAX_DELAY_MS);

    let dials_before = transport.dial_addrs().len();
    assert_eq!(
        dial_when_ready(&membership, &mut ledger, addr(A)).await,
        JoinDial::Exhausted
    );
    assert_eq!(
        transport.dial_addrs().len(),
        dials_before,
        "no dial when exhausted"
    );
    assert_eq!(transport.dial_addrs().len(), JOIN_MAX_ATTEMPTS + 3);
}

/// Only a failed dial grows the backoff. Answered dials and failed sends
/// leave it as it is, and a failed send allows the next attempt at once.
#[tokio::test(start_paused = true)]
async fn only_a_failed_dial_grows_the_backoff() {
    let mut a = vec![answers(peer(1)); 4];
    a.push(fails());
    let (membership, _transport) = membership(Script {
        dials: HashMap::from([(addr(A), a)]),
        join_sends: HashMap::from([(peer(1), vec![false])]),
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();
    for _ in 0..4 {
        assert_eq!(
            membership.join_dial(&mut ledger, addr(A)).await,
            JoinDial::Answered(peer(1))
        );
        assert_eq!(
            membership.join_send(addr(A), peer(1)).await.unwrap(),
            JoinSend::SendFailed
        );
        assert_eq!(ledger.delay_ms(addr(A)), JOIN_INITIAL_DELAY_MS);
    }
    let JoinDial::Failed { retry_after, .. } = membership.join_dial(&mut ledger, addr(A)).await
    else {
        panic!("the fifth dial fails");
    };
    let waited = retry_after.as_millis() as u64;
    assert!(
        (JOIN_INITIAL_DELAY_MS..=JOIN_INITIAL_DELAY_MS + JOIN_INITIAL_DELAY_MS / 8)
            .contains(&waited),
        "{waited} ms"
    );
    assert_eq!(ledger.delay_ms(addr(A)), 2 * JOIN_INITIAL_DELAY_MS);
    assert_eq!(ledger.attempts(addr(A)), 5);
}

/// A caller that refuses the answered peer (for example, it is not admitted
/// yet) ends the attempt before the JOIN frame: no frame, no stored
/// address, no active-view entry. The attempt counts; the backoff does not
/// change.
#[tokio::test(start_paused = true)]
async fn a_gate_refusal_before_the_send_leaves_no_trace() {
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(addr(A), vec![answers(peer(1))])]),
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(peer(1))
    );
    // The gate refuses: no join_send.
    assert!(transport.join_frames().is_empty());
    assert_eq!(transport.join_send_calls(), 0);
    assert_eq!(membership.get_peer_addr(&peer(1)).await, None);
    assert!(membership.active_view().is_empty());
    assert_eq!(ledger.attempts(addr(A)), 1);
    assert_eq!(ledger.delay_ms(addr(A)), JOIN_INITIAL_DELAY_MS);
}

/// Each step can be cancelled by dropping its future. A dropped dial counts
/// as an attempt and leaves the backoff as it is. A send dropped before it
/// completes sends no frame and does not change the active view.
#[tokio::test(start_paused = true)]
async fn the_steps_are_safe_to_drop() {
    let slow = Duration::from_secs(60);
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(
            addr(A),
            vec![
                Dial {
                    took: slow,
                    answer: Some(peer(1)),
                },
                answers(peer(1)),
            ],
        )]),
        join_send_took: slow,
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();

    let dial = tokio::time::timeout(
        Duration::from_secs(1),
        membership.join_dial(&mut ledger, addr(A)),
    )
    .await;
    assert!(dial.is_err(), "the slow dial is cancelled");
    assert_eq!(ledger.attempts(addr(A)), 1);
    assert_eq!(ledger.delay_ms(addr(A)), JOIN_INITIAL_DELAY_MS);

    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(peer(1))
    );
    let send = tokio::time::timeout(
        Duration::from_secs(1),
        membership.join_send(addr(A), peer(1)),
    )
    .await;
    assert!(send.is_err(), "the slow send is cancelled");
    assert!(transport.join_frames().is_empty());
    assert!(membership.active_view().is_empty());
    assert_eq!(ledger.attempts(addr(A)), 2);
}

/// `join_dial` reports the last attempt, and after it no dial is made.
#[tokio::test(start_paused = true)]
async fn the_last_attempt_is_reported_and_then_no_dial() {
    let (membership, transport) = membership(Script::default());
    let mut ledger = JoinLedger::new();
    let mut retries = Vec::new();
    for _ in 0..JOIN_MAX_ATTEMPTS {
        match dial_when_ready(&membership, &mut ledger, addr(A)).await {
            JoinDial::Failed {
                retry_after,
                exhausted,
            } => retries.push((retry_after.as_millis() as u64, exhausted)),
            other => panic!("{other:?}"),
        }
    }
    let bases = [100, 200, 400, 800, 1600, 3200, 6400, 12_800, 25_600, 30_000];
    for (i, ((waited, exhausted), base)) in retries.iter().zip(bases).enumerate() {
        assert!(
            *waited >= base && *waited <= base + base / 8,
            "retry {i}: {waited} ms"
        );
        assert_eq!(*exhausted, i + 1 == JOIN_MAX_ATTEMPTS);
    }
    assert_eq!(
        dial_when_ready(&membership, &mut ledger, addr(A)).await,
        JoinDial::Exhausted
    );
    assert_eq!(transport.dial_addrs().len(), JOIN_MAX_ATTEMPTS);
}

/// A `join_dial` before the seed's backoff ends makes no dial and uses no
/// attempt: saorsa-gossip enforces the wait.
#[tokio::test(start_paused = true)]
async fn an_early_dial_is_refused_without_using_an_attempt() {
    let (membership, transport) = membership(Script::default());
    let mut ledger = JoinLedger::new();
    let JoinDial::Failed { .. } = membership.join_dial(&mut ledger, addr(A)).await else {
        panic!("the first dial fails");
    };
    let ready_at = ledger.ready_at(addr(A)).unwrap();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::NotReady { ready_at }
    );
    assert_eq!(ledger.attempts(addr(A)), 1);
    assert_eq!(transport.dial_addrs().len(), 1);
}

/// Codex re-review of a2100a7, finding 2: a dropped `join_send` does not
/// leave a half-updated view. The active view is full and another task
/// holds the passive-view lock. The send is dropped after the JOIN frame
/// went out, while the local commit waits for that lock. The commit takes
/// every lock before its first change, so nothing changed: no peer is lost,
/// and the new peer is neither active nor alive. (At a2100a7 the demoted
/// peer was in neither view.)
#[tokio::test(start_paused = true)]
async fn a_dropped_send_leaves_a_consistent_view_when_the_passive_lock_is_held() {
    let joining = peer(99);
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(addr(A), vec![answers(joining)])]),
        ..Script::default()
    });
    let before: Vec<PeerId> = (1..=DEFAULT_ACTIVE_DEGREE as u8).map(peer).collect();
    for existing in &before {
        membership.add_active(*existing).await.unwrap();
    }
    assert_eq!(membership.active_view().len(), DEFAULT_ACTIVE_DEGREE);

    let passive_guard = membership.passive.write().await;
    let mut ledger = JoinLedger::new();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(joining)
    );
    let send = tokio::time::timeout(
        Duration::from_secs(1),
        membership.join_send(addr(A), joining),
    )
    .await;
    assert!(
        send.is_err(),
        "scenario: the send is dropped while its local commit waits for the passive lock"
    );
    assert_eq!(
        transport.join_frames().len(),
        1,
        "scenario: the JOIN frame went out"
    );
    drop(passive_guard);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let active = membership.active_view();
    let passive = membership.passive_view();
    for existing in &before {
        assert!(
            active.contains(existing) || passive.contains(existing),
            "peer {existing} is in neither view"
        );
    }
    assert_eq!(
        sorted(active),
        sorted(before),
        "the active view is unchanged"
    );
    assert!(
        passive.is_empty(),
        "the passive view is unchanged: {passive:?}"
    );
    assert_eq!(membership.swim().get_state(&joining).await, None);
}

/// Codex re-review of a2100a7, finding 2, second boundary: the send is
/// dropped while the local commit waits for the SWIM state lock. The new
/// peer must be active only if it is alive in SWIM. The commit takes every
/// lock before its first change, so nothing changed: the new peer is
/// neither active nor alive. (At a2100a7 it was active and not alive.)
#[tokio::test(start_paused = true)]
async fn a_dropped_send_leaves_a_consistent_view_when_the_swim_lock_is_held() {
    let joining = peer(99);
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(addr(A), vec![answers(joining)])]),
        ..Script::default()
    });
    let swim_guard = membership.swim.states.write().await;
    let mut ledger = JoinLedger::new();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(joining)
    );
    let send = tokio::time::timeout(
        Duration::from_secs(1),
        membership.join_send(addr(A), joining),
    )
    .await;
    assert!(
        send.is_err(),
        "scenario: the send is dropped while its local commit waits for the SWIM lock"
    );
    assert_eq!(
        transport.join_frames().len(),
        1,
        "scenario: the JOIN frame went out"
    );
    drop(swim_guard);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let active = membership.active_view().contains(&joining);
    let alive = membership.swim().get_state(&joining).await == Some(PeerState::Alive);
    assert_eq!(active, alive, "active {active}, alive {alive}");
    assert!(!active, "the commit did not start its changes");
}

/// The commit with a full active view has the same effect as the 0.5.87
/// `add_active`: one peer is demoted to the passive view, the new peer is
/// active and alive, and no peer is lost. (The two runs can demote
/// different peers: each view is a `HashSet` with its own hash seed.)
#[tokio::test(start_paused = true)]
async fn a_full_active_view_demotes_one_peer_as_in_0_5_87() {
    let joining = peer(99);
    let before: Vec<PeerId> = (1..=DEFAULT_ACTIVE_DEGREE as u8).map(peer).collect();
    let mut views = Vec::new();
    for reference in [true, false] {
        let (membership, _transport) = membership(Script::default());
        for existing in &before {
            membership.add_active(*existing).await.unwrap();
        }
        if reference {
            reference_add_active_0_5_87(&membership, joining)
                .await
                .unwrap();
        } else {
            membership.add_active(joining).await.unwrap();
        }
        let active = membership.active_view();
        let passive = membership.passive_view();
        assert_eq!(active.len(), DEFAULT_ACTIVE_DEGREE);
        assert!(active.contains(&joining));
        assert_eq!(passive.len(), 1);
        let mut all = active.clone();
        all.extend(passive.iter().copied());
        assert_eq!(
            sorted(all),
            sorted([before.clone(), vec![joining]].concat())
        );
        views.push((
            active.len(),
            passive.len(),
            membership.swim().get_state(&joining).await,
        ));
    }
    assert_eq!(views[0], views[1]);
}

/// The SWIM state of `peer`, read without awaiting (the caller holds no
/// SWIM lock).
fn swim_state<T: GossipTransport + 'static>(
    membership: &HyParViewMembership<T>,
    peer: PeerId,
) -> Option<PeerState> {
    membership
        .swim
        .states
        .try_read()
        .expect("no other holder of the SWIM state lock")
        .get(&peer)
        .map(|entry| entry.state)
}

fn sorted(mut peers: Vec<PeerId>) -> Vec<PeerId> {
    peers.sort_by_key(|peer| peer.to_bytes());
    peers
}

/// Poll `future` to completion on this thread, outside any Tokio runtime
/// context.
fn poll_outside_runtime<F: std::future::Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "scenario: no Tokio runtime is entered on this thread"
    );
    let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

/// Codex re-review of afabe92, finding 3: the legacy `Membership::join`
/// must not need an entered Tokio runtime on its success path; the 0.5.87
/// loop did not. The membership is built inside a runtime (its background
/// tasks need one). Then an all-ready JOIN is polled on a plain thread.
#[test]
fn legacy_join_succeeds_when_polled_outside_a_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (membership, transport) = runtime.block_on(async {
        membership(Script {
            dials: HashMap::from([(addr(A), vec![answers(peer(1))])]),
            ..Script::default()
        })
    });

    let result = poll_outside_runtime(membership.join(vec![addr(A).to_string()]));

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(transport.join_frames().len(), 1);
    assert_eq!(membership.active_view(), vec![peer(1)]);
    assert_eq!(swim_state(&membership, peer(1)), Some(PeerState::Alive));
    drop(runtime);
}

/// Codex re-review of afabe92, finding 3: the runtime that drives a
/// `join_send` shuts down while the local commit waits for a lock. The
/// active view is full and another holder keeps the passive-view lock. The
/// JOIN frame goes out, then the runtime shuts down. The views and the SWIM
/// state must be as before: no peer is lost, and the new peer is neither
/// active nor alive.
#[test]
fn a_runtime_shutdown_during_the_local_commit_leaves_the_views_unchanged() {
    let joining = peer(99);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let (membership, transport) = runtime.block_on(async {
        membership(Script {
            dials: HashMap::from([(addr(A), vec![answers(joining)])]),
            ..Script::default()
        })
    });
    let membership = Arc::new(membership);
    let before: Vec<PeerId> = (1..=DEFAULT_ACTIVE_DEGREE as u8).map(peer).collect();
    runtime.block_on(async {
        for existing in &before {
            membership.add_active(*existing).await.unwrap();
        }
    });
    let passive_guard = runtime.block_on(Arc::clone(&membership.passive).write_owned());
    let sender = Arc::clone(&membership);
    let _send = runtime.spawn(async move {
        let mut ledger = JoinLedger::new();
        let _ = sender.join_dial(&mut ledger, addr(A)).await;
        sender.join_send(addr(A), joining).await
    });
    // The send runs until its local commit waits for the passive lock.
    runtime.block_on(async { tokio::time::sleep(Duration::from_millis(10)).await });
    assert_eq!(
        transport.join_frames().len(),
        1,
        "scenario: the JOIN frame went out"
    );

    drop(runtime);
    drop(passive_guard);

    assert_eq!(sorted(membership.active_view()), sorted(before));
    assert!(membership.passive_view().is_empty());
    assert_eq!(swim_state(&membership, joining), None);
}

/// Codex re-review of 24eb1c4, finding 1: a commit that waits for the SWIM
/// lock must not hide the active view. `active_view()` uses `try_read` and
/// returns an empty list when the active-view lock is held, so a
/// coordinator advert or a FOAF lookup that reads it then would send
/// nothing. The view holds A and has room; another holder keeps the SWIM
/// lock while `add_active(B)` waits. Readers must still see [A]. Once the
/// lock is free, B is active and alive.
#[tokio::test(start_paused = true)]
async fn the_active_view_stays_readable_while_a_commit_waits_for_the_swim_lock() {
    let (membership, _transport) = membership(Script::default());
    membership.add_active(peer(1)).await.unwrap();
    let membership = Arc::new(membership);
    let swim_guard = Arc::clone(&membership.swim.states).write_owned().await;
    let adder = {
        let membership = Arc::clone(&membership);
        tokio::spawn(async move { membership.add_active(peer(2)).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !adder.is_finished(),
        "scenario: add_active(B) waits for the SWIM lock"
    );

    assert_eq!(membership.active_view(), vec![peer(1)]);
    assert!(membership.passive_view().is_empty());

    drop(swim_guard);
    adder.await.unwrap().unwrap();
    assert_eq!(
        sorted(membership.active_view()),
        sorted(vec![peer(1), peer(2)])
    );
    assert_eq!(swim_state(&membership, peer(2)), Some(PeerState::Alive));
}

/// The time from now until `at`, in milliseconds.
fn millis_until(at: TokioInstant) -> u64 {
    at.duration_since(TokioInstant::now()).as_millis() as u64
}

/// D252: the loss cooldown. A peer that answered the dial and then lost its
/// connection is reported with `record_loss`. The next attempt on that seed
/// waits 100 ms, then 200 ms, and so on up to 30 s, plus at most 1/8
/// jitter. A dial before then makes no dial and uses no attempt. Answered
/// dials do not reset the cooldown, and losses do not change the dial
/// backoff.
#[tokio::test(start_paused = true)]
async fn record_loss_paces_a_seed_that_answers_and_closes() {
    let (membership, transport) = membership(Script {
        dials: HashMap::from([(addr(A), vec![answers(peer(1))])]),
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();
    let mut cooldown = JOIN_INITIAL_DELAY_MS;
    for attempt in 1..=JOIN_MAX_ATTEMPTS {
        assert_eq!(
            membership.join_dial(&mut ledger, addr(A)).await,
            JoinDial::Answered(peer(1))
        );
        let ready_at = ledger.record_loss(addr(A));
        let waited = millis_until(ready_at);
        assert!(
            waited >= cooldown && waited <= cooldown + cooldown / 8,
            "loss {attempt}: cooldown {waited} ms, expected {cooldown} ms plus at most 1/8 jitter"
        );
        assert_eq!(ledger.ready_at(addr(A)), Some(ready_at));
        if attempt < JOIN_MAX_ATTEMPTS {
            let dials = transport.dial_addrs().len();
            assert_eq!(
                membership.join_dial(&mut ledger, addr(A)).await,
                JoinDial::NotReady { ready_at },
                "no dial before the cooldown ends"
            );
            assert_eq!(transport.dial_addrs().len(), dials);
            assert_eq!(ledger.attempts(addr(A)), attempt);
            tokio::time::sleep_until(ready_at).await;
        }
        cooldown = (cooldown * 2).min(JOIN_MAX_DELAY_MS);
        assert_eq!(ledger.delay_ms(addr(A)), JOIN_INITIAL_DELAY_MS);
    }
    assert_eq!(transport.dial_addrs().len(), JOIN_MAX_ATTEMPTS);
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Exhausted
    );
    assert!(transport.join_frames().is_empty());
}

/// D252: the readiness is the later of the dial backoff and the loss
/// cooldown; the delays are not added.
#[tokio::test(start_paused = true)]
async fn the_later_of_backoff_and_loss_cooldown_wins() {
    let (membership, _transport) = membership(Script::default());
    let mut ledger = JoinLedger::new();
    // Four failed dials: the next dial backoff is 1.6 s.
    for _ in 0..4 {
        if let JoinDial::Failed { retry_after, .. } =
            membership.join_dial(&mut ledger, addr(A)).await
        {
            tokio::time::sleep(retry_after).await;
        }
    }
    let JoinDial::Failed { retry_after, .. } = membership.join_dial(&mut ledger, addr(A)).await
    else {
        panic!("the fifth dial fails");
    };
    let dial_ready = ledger.ready_at(addr(A)).unwrap();
    assert_eq!(millis_until(dial_ready), retry_after.as_millis() as u64);
    // A loss now (100 ms cooldown) ends before the 1.6 s backoff: the
    // backoff wins, and the two are not added.
    let ready = ledger.record_loss(addr(A));
    assert_eq!(ready, dial_ready);
    assert_eq!(ledger.loss_delay_ms(addr(A)), 2 * JOIN_INITIAL_DELAY_MS);
    assert_eq!(ledger.delay_ms(addr(A)), 3200);
}

/// D252: the loss cooldown and the attempts belong to the seed address,
/// across candidate changes: another seed is unaffected and can be dialled
/// while the lost one waits.
#[tokio::test(start_paused = true)]
async fn a_lost_seed_waits_while_another_seed_is_dialled() {
    let (membership, transport) = membership(Script {
        dials: HashMap::from([
            (addr(A), vec![answers(peer(1))]),
            (addr(B), vec![answers(peer(2))]),
        ]),
        ..Script::default()
    });
    let mut ledger = JoinLedger::new();
    assert_eq!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::Answered(peer(1))
    );
    let a_ready = ledger.record_loss(addr(A));
    assert!(matches!(
        membership.join_dial(&mut ledger, addr(A)).await,
        JoinDial::NotReady { .. }
    ));
    assert_eq!(
        membership.join_dial(&mut ledger, addr(B)).await,
        JoinDial::Answered(peer(2))
    );
    assert_eq!(
        membership.join_send(addr(B), peer(2)).await.unwrap(),
        JoinSend::Joined
    );
    assert_eq!(ledger.ready_at(addr(B)), None);
    assert_eq!(ledger.ready_at(addr(A)), Some(a_ready));
    assert_eq!(transport.dial_addrs(), vec![addr(A), addr(B)]);
}
