use bevy_ecs::schedule::{InternedScheduleLabel, ScheduleLabel};
use bevy_ecs::world::World;
use bevy_ecs::{prelude::*, world::CommandQueue};
use bevy_time::{Time, Virtual};
use std::time::Duration;

/// Resource controlling how often the Dioxus sync schedules run.
///
/// The `timestep` is derived from the `dioxus_sync_fps` set on
/// [`DioxusBevyMirrorPlugin`](crate::DioxusBevyMirrorPlugin).
#[derive(Resource, Clone, Debug)]
pub struct DioxusSyncConfig {
    /// The interval between successive runs of the Dioxus sync schedules.
    pub timestep: Duration,
}

impl Default for DioxusSyncConfig {
    fn default() -> Self {
        Self {
            timestep: Duration::from_secs_f64(1.0 / 60.0),
        }
    }
}

impl DioxusSyncConfig {
    /// Create from a target frames-per-second.
    pub fn from_fps(fps: u32) -> Self {
        Self {
            timestep: Duration::from_secs_f64(1.0 / fps as f64),
        }
    }
}

/// Accumulated time since the last Dioxus sync tick.
///
/// Used internally by [`DioxusSyncMain::run_dioxus_sync_main`] to track when
/// enough [`Time<Virtual>`] has elapsed to warrant another sync tick.
#[derive(Resource, Default)]
pub(crate) struct DioxusSyncAccumulator {
    pub accumulated: Duration,
}
/// The container schedule that runs the four Dioxus sync sub-schedules in order.
///
/// It is driven by [`DioxusSyncMain::run_dioxus_sync_main`] at the rate
/// specified in [`DioxusSyncConfig`].
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncMain;

/// Systems that must run at the start of a Dioxus sync tick.
///
/// [`process_commands`](crate::process_commands) lives here so that Dioxus
/// commands are available to the sync systems that follow.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncPreUpdate;

/// Main Dioxus sync systems all `drive_*` signal tickers.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncUpdate;

/// Mirror world synchronisation systems (e.g. `sync_mirror_to_resource`).
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncPostUpdate;

/// Cleanup systems that run at the end of a Dioxus sync tick.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncLast;

/// Execution order of the sub-schedules inside [`DioxusSyncMain`].
#[derive(Resource, Debug)]
pub struct DioxusSyncMainScheduleOrder {
    /// Sub-schedule labels in execution order.
    pub labels: Vec<InternedScheduleLabel>,
}

impl Default for DioxusSyncMainScheduleOrder {
    fn default() -> Self {
        Self {
            labels: vec![
                DioxusSyncPreUpdate.intern(),
                DioxusSyncUpdate.intern(),
                DioxusSyncPostUpdate.intern(),
                DioxusSyncLast.intern(),
            ],
        }
    }
}

impl DioxusSyncMain {
    /// System that runs [`DioxusSyncMain`] at the rate configured in
    /// [`DioxusSyncConfig`].
    ///
    /// Added to the [`PostUpdate`] schedule so it is checked every frame, but
    /// the contained sync schedules only tick when enough
    /// [`Time<Virtual>`] delta has accumulated.
    pub fn run_dioxus_sync_main(world: &mut World) {
        // Process pending Dioxus commands BEFORE ticking sync schedules.
        // This ensures any dynamically-added systems (resource mirrors, etc.)
        // are registered and available for the sub-schedules in this frame.
        {
            use crate::CommandQueueReciever;
            let rx = &world.resource::<CommandQueueReciever>().rx;
            let mut queue = CommandQueue::default();
            while let Ok(mut cmd) = rx.try_recv() {
                queue.append(&mut cmd);
            }
            queue.apply(world);
        }

        let delta = world.resource::<Time<Virtual>>().delta();
        let timestep = world.resource::<DioxusSyncConfig>().timestep;

        // Accumulate virtual time.
        world.resource_mut::<DioxusSyncAccumulator>().accumulated += delta;

        loop {
            let should_run = {
                let acc = world.resource::<DioxusSyncAccumulator>();
                acc.accumulated >= timestep
            };

            if !should_run {
                break;
            }

            // Consume one timestep worth of accumulated time.
            world.resource_mut::<DioxusSyncAccumulator>().accumulated -= timestep;

            // Run the four sync schedules in order.
            let labels = world
                .resource::<DioxusSyncMainScheduleOrder>()
                .labels
                .clone();

            for &label in &labels {
                let _ = world.try_run_schedule(label);
            }
        }
    }
}
