use bevy_ecs::schedule::{InternedScheduleLabel, ScheduleLabel};
use bevy_ecs::world::World;
use bevy_ecs::{prelude::*, world::CommandQueue};
use bevy_time::{Time, Virtual};
use std::time::Duration;

/// Controls how often the dioxus sync schedules run.
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

/// Accumulated time since the last dioxus sync tick.
#[derive(Resource, Default)]
pub(crate) struct DioxusSyncAccumulator {
    pub accumulated: Duration,
}
/// Container schedule that runs the four dioxus sync sub-schedules in order.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncMain;

/// Systems that run at the start of a dioxus sync tick.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncPreUpdate;

/// Main dioxus sync systems, including all signal ticker drivers.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncUpdate;

/// Mirror world synchronization systems.
#[derive(ScheduleLabel, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct DioxusSyncPostUpdate;

/// Cleanup systems that run at the end of a dioxus sync tick.
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
    /// Checked every frame. Sync schedules tick when enough time has elapsed.
    pub fn run_dioxus_sync_main(world: &mut World) {
        // Process pending dioxus commands before ticking sync schedule so they are visible to other systems in time.
        {
            use crate::CommandQueueReceiver;
            let rx = &world.resource::<CommandQueueReceiver>().rx;
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
