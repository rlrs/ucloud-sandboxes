from dataclasses import replace
from datetime import timedelta
import unittest

from ucloud_sandboxes.models import (
    LiveScaleSignals, NodeRuntimeMetrics, ResourceQuantity, SandboxDemand,
    SandboxInventoryEntry, SandboxPlacementRequest, ScalePolicy, utc_now,
)
from ucloud_sandboxes.policy import evaluate_scale
from tests.test_policy import node


class MemoryForecastPolicyTests(unittest.TestCase):
    def setUp(self):
        self.now = utc_now()
        self.total = ResourceQuantity(vcpu=32,memory_mb=98304,disk_mb=1449984)
        self.policy = ScalePolicy(max_nodes=8,max_create_per_cycle=8,
            max_provisioning_nodes=8, default_node_resources=self.total)

    def worker(self, identity, memory=0, active=1, **kwargs):
        return node(identity, total_resources=self.total, active=active,
            job_cpu=32, job_memory_gb=96,
            runtime_metrics=NodeRuntimeMetrics(collected_at=self.now,
                memory_total_mb=89900,memory_available_mb=89900-memory,
                memory_working_set_mb=memory), **kwargs)

    def pending(self, count):
        return SandboxDemand(placement_requests=(SandboxPlacementRequest(count=count,
            resources=ResourceQuantity(vcpu=8,memory_mb=2048,disk_mb=4096)),))

    def test_dense_parked_owners_do_not_pin_memory_or_block_new_work(self):
        worker = self.worker('dense',memory=6000,active=0)
        inventory = tuple(SandboxInventoryEntry(f'sandbox-{i}',1,f'create-{i}',
            'a'*64,'parked',ResourceQuantity(memory_mb=2048)) for i in range(512))
        worker = replace(worker,heartbeat=replace(worker.heartbeat,inventory=inventory,inventory_complete=True))
        decision = evaluate_scale([worker],self.pending(16),self.policy,now=self.now)
        self.assertEqual(decision.creates,0)
        self.assertEqual(decision.desired_resources.memory_mb,7500+16*2560)

    def test_unknown_cold_burst_then_actual_working_memory_releases_forecast(self):
        initial = evaluate_scale([self.worker('idle',active=0)],self.pending(256),
                                 self.policy,now=self.now)
        self.assertEqual(initial.creates,7)
        self.assertEqual(initial.desired_resources.memory_mb,655360)
        # Same256owners later use384GiB; their2GiBdeclaredlimits do notpin8nodes.
        ready = [self.worker(str(i),memory=65536,active=43) for i in range(6)]
        decision = evaluate_scale(ready,SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,491520)
        self.assertEqual(decision.creates,0)
        self.assertEqual(decision.projected_free_resources.memory_mb,6*89900)

    def test_transient_promises_and_prepared_demand_count_once(self):
        worker = self.worker('resident',memory=30000)
        worker = replace(worker,heartbeat=replace(worker.heartbeat,
            reserved_resources=ResourceQuantity(memory_mb=4096),
            build_reserved_resources=ResourceQuantity(memory_mb=1024)))
        prepared = replace(self.pending(8),placement_requests=(),
                           prepared_placement_requests=self.pending(8).placement_requests)
        decision = evaluate_scale([worker],prepared,self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,43900+8*2560)
        idle = replace(worker,active_sandboxes=0,heartbeat=replace(worker.heartbeat,
            active_sandboxes=0,runtime_metrics=replace(worker.heartbeat.runtime_metrics,
                memory_working_set_mb=0,memory_available_mb=89900)))
        self.assertEqual(evaluate_scale([idle],SandboxDemand(),self.policy,
            now=self.now).desired_resources.memory_mb,6400)

    def test_host_used_memory_tightens_smaller_working_set(self):
        worker = self.worker('used',memory=72000)
        worker = replace(worker,heartbeat=replace(worker.heartbeat,runtime_metrics=replace(
            worker.heartbeat.runtime_metrics,memory_working_set_mb=100)))
        decision = evaluate_scale([worker],SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,90000)
        self.assertEqual(decision.creates,1)

    def test_stale_observation_does_not_invent_parked_limits(self):
        worker = self.worker('stale',memory=85000)
        inventory = (SandboxInventoryEntry('parked',1,'create','a'*64,'parked',
                     ResourceQuantity(memory_mb=50000)),)
        worker = replace(worker,heartbeat=replace(worker.heartbeat,inventory=inventory,inventory_complete=True,
            runtime_metrics=replace(worker.heartbeat.runtime_metrics,
                collected_at=self.now-timedelta(seconds=31))))
        self.assertEqual(evaluate_scale([worker],SandboxDemand(),self.policy,
                         now=self.now).desired_resources.memory_mb,89900)

    def test_pressure_can_expand_while_first_worker_is_booting(self):
        busy = [self.worker(str(i),memory=20000) for i in range(4)]
        booting = node('booting',state='IN_QUEUE',heartbeat_present=False,
                       job_cpu=32,job_memory_gb=96)
        pressure = LiveScaleSignals(pressure_samples=3,latest_pressure_age_seconds=1,
                                    io_psi_full_avg10=30)
        policy = replace(self.policy,max_create_per_cycle=2,max_provisioning_nodes=3)
        decision = evaluate_scale([*busy,booting],SandboxDemand(),policy,
                                  live_signals=pressure,now=self.now)
        self.assertEqual(decision.creates,1)
        self.assertEqual(decision.projected_free_resources.memory_mb,5*89900)
        capped = evaluate_scale([*busy,booting],SandboxDemand(),replace(policy,max_nodes=5),
                                live_signals=pressure,now=self.now)
        self.assertEqual(capped.creates,0)

    def test_create_pressure_never_clips_target_below_busy_fleet(self):
        busy = [self.worker(str(i),memory=20000) for i in range(4)]
        pressure = LiveScaleSignals(pressure_samples=3,latest_pressure_age_seconds=1,
            io_psi_full_avg10=30,create_pressure_samples=3,latest_create_pressure_age_seconds=1,
            sandbox_create_limit=8,sandbox_create_rejections=10)
        decision = evaluate_scale(busy,SandboxDemand(),self.policy,
                                  live_signals=pressure,now=self.now)
        self.assertEqual(decision.creates,1)
        idle = self.worker('idle',active=0)
        self.assertEqual(evaluate_scale([*busy,idle],SandboxDemand(),self.policy,
            live_signals=pressure,now=self.now).creates,0)

    def test_scale_down_uses_same_memory_units_and_pressure_cooldown(self):
        busy = self.worker('busy',memory=65000)
        idle = self.worker('idle',active=0,idle_since=self.now-timedelta(seconds=1000))
        policy = replace(self.policy,warm_resources=ResourceQuantity(memory_mb=16000))
        self.assertEqual(evaluate_scale([busy,idle],SandboxDemand(),policy,now=self.now).stops,())
        cooled = replace(policy,warm_resources=ResourceQuantity())
        self.assertEqual(evaluate_scale([busy,idle],SandboxDemand(),cooled,now=self.now).stops,('idle',))
        pressure = LiveScaleSignals(pressure_samples=1,latest_pressure_age_seconds=120)
        self.assertEqual(evaluate_scale([busy,idle],SandboxDemand(),cooled,
            live_signals=pressure,now=self.now).stops,())

    def test_unknown_occupied_worker_has_no_spare_memory_credit(self):
        worker = self.worker('unknown',memory=20000)
        worker = replace(worker,heartbeat=replace(worker.heartbeat,runtime_metrics=None,
            inventory_complete=False))
        idle = evaluate_scale([worker],SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(idle.desired_resources.memory_mb,self.total.memory_mb)
        self.assertEqual(idle.creates,0)
        self.assertEqual(evaluate_scale([worker],self.pending(1),self.policy,
                         now=self.now).creates,1)
        promised = replace(worker,heartbeat=replace(worker.heartbeat,
            reserved_resources=ResourceQuantity(memory_mb=2048)))
        self.assertEqual(evaluate_scale([promised],SandboxDemand(),self.policy,
            now=self.now).desired_resources.memory_mb,self.total.memory_mb)

    def test_idle_parked_inventory_still_has_measured_cache_footprint(self):
        worker = self.worker('parked',memory=72000,active=0)
        item = SandboxInventoryEntry('parked',1,'create','a'*64,'parked',
                                     ResourceQuantity(memory_mb=2048))
        worker = replace(worker,heartbeat=replace(worker.heartbeat,inventory=(item,)))
        decision = evaluate_scale([worker],SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,90000)
        self.assertEqual(decision.creates,1)

    def test_complete_but_older_empty_inventory_does_not_erase_new_active_route(self):
        worker = self.worker('new-route',memory=20000)
        worker = replace(worker,heartbeat=replace(worker.heartbeat,
            inventory_complete=True, runtime_metrics=replace(worker.heartbeat.runtime_metrics,
                collected_at=self.now-timedelta(seconds=31))))
        decision = evaluate_scale([worker],SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,89900)
        self.assertEqual(decision.creates,0)

    def test_stale_complete_inventory_counts_transition_promise_once(self):
        worker = self.worker('transition',memory=20000)
        item = SandboxInventoryEntry('starting',1,'create','a'*64,'creating',
                                     ResourceQuantity(memory_mb=2048))
        worker = replace(worker,heartbeat=replace(worker.heartbeat,
            inventory_complete=True,inventory=(item,),reserved_resources=ResourceQuantity(memory_mb=2048),
            runtime_metrics=replace(worker.heartbeat.runtime_metrics,
                collected_at=self.now-timedelta(seconds=31))))
        decision = evaluate_scale([worker],SandboxDemand(),self.policy,now=self.now)
        self.assertEqual(decision.desired_resources.memory_mb,2560)
