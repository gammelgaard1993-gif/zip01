"""
Concurrent load test for the Teton streaming backend.

What it measures:
  - Throughput (requests/sec actually delivered)
  - Alarm delivery latency: time from fall_warn POST 200 OK → SSE receipt (p50/p95/p99)
    - Error rate and sampled server queue/latency metrics

Requires the development dependency aiohttp for pooled HTTP connections.

Usage:
    # Start service first: python main.py
    python _loadtest.py                          # 200 devices, 60s, baseline ~1 ev/dev/s
    python _loadtest.py --devices 500 --duration 120 --concurrency 32
    python _loadtest.py --burst --devices 200    # 10x burst for 30s mid-run
"""

import argparse
import asyncio
import json
import random
import statistics
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast, TypedDict
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import aiohttp

# Use 127.0.0.1 to bypass Windows IPv6 dual-stack resolution delay for localhost.
TARGET = "http://127.0.0.1:8080"


class MetricsSnapshot(TypedDict):
    elapsed_seconds: float
    phase: str
    counters: dict[str, int]


class _Jsonl:
    """Opt-in machine-readable progress stream for a controlling process.

    Enabled only when --jsonl is passed; otherwise every emit is a no-op so the human-readable
    CLI output is unchanged. Each record is one JSON object per line, flushed immediately so a
    parent process reading the pipe sees events in near-real-time (stdout is block-buffered when
    piped otherwise). Emission is deliberately coarse (progress ticks / metric snapshots, not
    per-event) so the pipe never becomes a bottleneck at burst rates.
    """

    def __init__(self, enabled: bool):
        self._enabled = enabled

    def emit(self, record: dict[str, Any]) -> None:
        if not self._enabled:
            return
        sys.stdout.write(json.dumps(record) + "\n")
        sys.stdout.flush()


class _NullJsonl(_Jsonl):
    def __init__(self) -> None:
        super().__init__(False)


def summarize_metrics(snapshots: list[MetricsSnapshot]) -> dict[str, int]:
    """Return counter deltas and queue/latency peaks from metric snapshots."""
    if not snapshots:
        return {}

    first = snapshots[0]["counters"]
    last = snapshots[-1]["counters"]
    keys = set().union(*(snapshot["counters"] for snapshot in snapshots))
    summary: dict[str, int] = {}
    for key in keys:
        if _is_gauge_metric(key):
            summary[f"max_{key}"] = max(snapshot["counters"].get(key, 0) for snapshot in snapshots)
            summary[f"final_{key}"] = last.get(key, 0)
        else:
            summary[f"delta_{key}"] = last.get(key, 0) - first.get(key, 0)
    return summary


def _is_gauge_metric(key: str) -> bool:
    """True for instantaneous measurements that must not be summarized as counter deltas."""
    return (
        "queue_depth" in key
        or key.endswith("_p95")
        or key.endswith("_active_device_buffers")
        or key.endswith("_pending_flushes")
        or key.endswith("_inflight_age_ms")
        or key.endswith("_last_batch_size")
    )


def build_pressure_graph(snapshots: list[MetricsSnapshot], width: int = 60) -> list[str]:
    """Render sampled ingress, writer, and worker queue pressure as fixed-width ASCII bars."""
    pressure_keys = {
        "Ingress": ("queue_depth_high", "queue_depth_normal"),
        "Writer": ("sqlite_writer_queue_depth_priority", "sqlite_writer_queue_depth_normal"),
        "Workers": ("worker_queue_depth_high", "worker_queue_depth_normal"),
    }
    if not snapshots:
        return []

    sample_count = min(width, len(snapshots))
    step = len(snapshots) / sample_count
    lines = ["  Queue pressure timeline (+ normal, # high/priority)"]
    for label, (high_key, normal_key) in pressure_keys.items():
        points: list[tuple[int, int]] = []
        for index in range(sample_count):
            start = int(index * step)
            end = max(start + 1, int((index + 1) * step))
            bucket = snapshots[start:end]
            high = max(snapshot["counters"].get(high_key, 0) for snapshot in bucket)
            normal = max(snapshot["counters"].get(normal_key, 0) for snapshot in bucket)
            points.append((high, normal))

        peak = max(high + normal for high, normal in points)
        if peak == 0:
            graph = "." * sample_count
        else:
            graph = "".join(
                "#" if high > 0 else "+" if normal > 0 else "."
                for high, normal in points
            )
        lines.append(f"    {label:<8} {graph}  peak={peak}")
    lines.append("    time -> each character is one sampled interval")
    return lines


def build_dashboard_html(
    snapshots: list[MetricsSnapshot], room_traffic: dict[str, dict[int, int]], rate_limit: int | None = None
) -> str:
    """Build a standalone browser dashboard from sampled pressure and generated room traffic."""
    snapshot_data = json.dumps(snapshots).replace("</", "<\\/")
    traffic_data = json.dumps(room_traffic).replace("</", "<\\/")
    rate_limit_data = json.dumps(rate_limit)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>zip01 Load Test Dashboard</title><link rel="stylesheet" href="styles.css"></head><body><main><h1>Load Test Dashboard</h1><p>Queue pressure is sampled from the service. Room traffic counts generated events dispatched by this test driver.</p>
<section class="controls"><label>Phase<select id="phase"><option value="all">All samples</option><option value="baseline">Baseline</option><option value="burst">Burst</option><option value="after">After drain</option></select></label><label>Pipeline focus<select id="focus"><option value="all">All stages</option><option value="ingress">Ingress</option><option value="writer">SQLite writer</option><option value="workers">Workers</option></select></label></section>
<section class="kpis"><article class="kpi"><span>Selected ingress peak</span><strong id="kpi-rate">-</strong></article><article class="kpi"><span>Selected queue peak</span><strong id="kpi-queue">-</strong></article><article class="kpi"><span>Oldest work peak</span><strong id="kpi-age">-</strong></article><article class="kpi"><span>Correctness failures</span><strong id="kpi-failures">-</strong></article></section>
<section class="grid"><article class="panel"><h2>Ingress Requests / Second</h2><canvas id="ingress"></canvas><div class="legend"><span><i class="dot dot-blue"></i>accepted req/s</span><span><i class="dot dot-high"></i>queue pressure</span></div></article>
<article class="panel"><h2>SQLite Writer Batches / Second</h2><canvas id="writer"></canvas><div class="legend"><span><i class="dot dot-orange"></i>committed batches/s</span><span><i class="dot dot-high"></i>queue pressure</span></div></article>
<article class="panel"><h2>Worker Events / Second</h2><canvas id="workers"></canvas><div class="legend"><span><i class="dot dot-green"></i>handled events/s</span><span><i class="dot dot-high"></i>queue pressure</span></div></article>
<article class="panel"><h2>Room Traffic Profile</h2><canvas id="room-profile"></canvas><div class="legend"><span>all rooms, sorted busiest to quietest</span></div></article>
<article class="panel"><h2>Top 10 Busiest Rooms</h2><canvas id="room-ranking"></canvas><div class="legend"><span>generated events dispatched by the driver</span></div></article></section><p class="note">Generated by helpers/_loadtest.py</p></main><div id="tip" class="tip"></div>
<script>let snapshots={snapshot_data}; const allSnapshots=snapshots, traffic={traffic_data}, fixedRateLimit={rate_limit_data};
const palette={{high:'#bd3043',blue:'#1677b8',orange:'#d56b22',green:'#178056',grid:'#d7d4ca',ink:'#182027',muted:'#64707a'}};
function context(id) {{ const canvas=document.getElementById(id), box=canvas.getBoundingClientRect(), ratio=devicePixelRatio||1; canvas.width=box.width*ratio; canvas.height=box.height*ratio; const ctx=canvas.getContext('2d'); ctx.scale(ratio,ratio); return [ctx,box.width,box.height]; }}
function stageChart(id, rateKey, highKey, normalKey, color) {{ const [ctx,w,h]=context(id), pad={{l:46,r:40,t:26,b:32}}, count=snapshots.length, rates=[], queues=[], canvas=document.getElementById(id), tip=document.getElementById('tip'); for(let index=0;index<count;index++){{const current=snapshots[index], prior=snapshots[Math.max(0,index-1)], seconds=Math.max(.001,current.elapsed_seconds-prior.elapsed_seconds);rates.push(index?Math.max(0,(current.counters[rateKey]||0)-(prior.counters[rateKey]||0))/seconds:0);queues.push((current.counters[highKey]||0)+(current.counters[normalKey]||0));}} const ratePeak=Math.max(1,...rates), queuePeak=Math.max(1,...queues), plotW=w-pad.l-pad.r, plotH=h-pad.t-pad.b, x=(index)=>pad.l+plotW*(count<=1?0:index/(count-1)), rateY=(value)=>pad.t+plotH*(1-value/ratePeak), queueY=(value)=>pad.t+plotH*(1-value/queuePeak), draw=()=>{{ctx.clearRect(0,0,w,h);ctx.font='11px ui-monospace,Consolas,monospace';const elapsed=Math.round(snapshots[count-1]?.elapsed_seconds||0);snapshots.forEach((sample,index)=>{{if(sample.phase==='burst'){{ctx.fillStyle='rgba(213,107,34,.08)';ctx.fillRect(x(index)-plotW/Math.max(1,count-1)/2,pad.t,plotW/Math.max(1,count-1),plotH);}}}});for(let row=0;row<5;row++){{const yy=pad.t+plotH*row/4, rate=Math.round(ratePeak*(4-row)/4), queue=Math.round(queuePeak*(4-row)/4);ctx.strokeStyle=row===4?'#aeb7ba':palette.grid;ctx.lineWidth=row===4?1.2:1;ctx.beginPath();ctx.moveTo(pad.l,yy);ctx.lineTo(w-pad.r,yy);ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign='right';ctx.fillText(String(rate),pad.l-6,yy+4);ctx.textAlign='left';ctx.fillText(String(queue),w-pad.r+6,yy+4);}}for(let tick=0;tick<=4;tick++){{const xx=pad.l+plotW*tick/4, second=Math.round(elapsed*tick/4);ctx.strokeStyle=palette.grid;ctx.beginPath();ctx.moveTo(xx,pad.t);ctx.lineTo(xx,pad.t+plotH);ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign=tick===0?'left':tick===4?'right':'center';ctx.fillText(`${{second}}s`,xx,h-8);}}ctx.fillStyle=palette.muted;ctx.textAlign='left';ctx.fillText('rate/s',pad.l,pad.t-10);ctx.textAlign='right';ctx.fillText('queue',w-pad.r,pad.t-10);if(!count)return;ctx.beginPath();queues.forEach((value,index)=>index?ctx.lineTo(x(index),queueY(value)):ctx.moveTo(x(index),queueY(value)));ctx.lineTo(x(count-1),pad.t+plotH);ctx.lineTo(x(0),pad.t+plotH);ctx.closePath();ctx.fillStyle='rgba(189,48,67,.16)';ctx.fill();ctx.strokeStyle=color;ctx.lineWidth=2.6;ctx.beginPath();rates.forEach((value,index)=>index?ctx.lineTo(x(index),rateY(value)):ctx.moveTo(x(index),rateY(value)));ctx.stroke();rates.forEach((value,index)=>{{ctx.fillStyle='#fffefa';ctx.beginPath();ctx.arc(x(index),rateY(value),3,0,Math.PI*2);ctx.fill();ctx.strokeStyle=color;ctx.lineWidth=1.8;ctx.stroke();}});}}; draw();canvas.addEventListener('mousemove',(event)=>{{if(!count)return;const rect=canvas.getBoundingClientRect(), position=Math.max(0,Math.min(plotW,event.clientX-rect.left-pad.l)), index=Math.round(position/plotW*Math.max(0,count-1)), sample=snapshots[index];ctx.save();draw();ctx.strokeStyle=palette.ink;ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(x(index),pad.t);ctx.lineTo(x(index),pad.t+plotH);ctx.stroke();ctx.restore();tip.innerHTML=`<b>${{Math.round(sample.elapsed_seconds)}}s</b><br>rate: ${{rates[index].toFixed(1)}}/s<br>queue: ${{queues[index]}}`;tip.style.display='block';tip.style.left=`${{event.clientX+14}}px`;tip.style.top=`${{event.clientY+14}}px`;}});canvas.addEventListener('mouseleave',()=>{{tip.style.display='none';draw();}}); }}
function niceRateLimit(peak) {{
    if (fixedRateLimit) return fixedRateLimit;
    const target=Math.max(100,peak*1.2), power=Math.pow(10,Math.floor(Math.log10(target))), fraction=target/power;
    return (fraction<=1?1:fraction<=2?2:fraction<=5?5:10)*power;
}}
function stageChart(id,rateKey,highKey,normalKey,color) {{
    const [ctx,w,h]=context(id),pad={{l:46,r:40,t:26,b:32}},count=snapshots.length,rates=[],queues=[],canvas=document.getElementById(id),tip=document.getElementById('tip');
    for(let index=0;index<count;index++){{const current=snapshots[index],prior=snapshots[Math.max(0,index-1)],seconds=Math.max(.001,current.elapsed_seconds-prior.elapsed_seconds);rates.push(index?Math.max(0,(current.counters[rateKey]||0)-(prior.counters[rateKey]||0))/seconds:0);queues.push((current.counters[highKey]||0)+(current.counters[normalKey]||0));}}
    const rateLimit=niceRateLimit(Math.max(1,...rates)),queuePeak=Math.max(1,...queues),plotW=w-pad.l-pad.r,plotH=h-pad.t-pad.b,x=index=>pad.l+plotW*(count<=1?0:index/(count-1)),rateY=value=>pad.t+plotH*(1-Math.min(value,rateLimit)/rateLimit),queueY=value=>pad.t+plotH*(1-value/queuePeak);
    const draw=()=>{{ctx.clearRect(0,0,w,h);ctx.font='11px ui-monospace,Consolas,monospace';const elapsed=Math.round(snapshots[count-1]?.elapsed_seconds||0);snapshots.forEach((sample,index)=>{{if(sample.phase==='burst'){{ctx.fillStyle='rgba(213,107,34,.08)';ctx.fillRect(x(index)-plotW/Math.max(1,count-1)/2,pad.t,plotW/Math.max(1,count-1),plotH);}}}});for(let row=0;row<5;row++){{const yy=pad.t+plotH*row/4,rate=Math.round(rateLimit*(4-row)/4),queue=Math.round(queuePeak*(4-row)/4);ctx.strokeStyle=row===4?'#aeb7ba':palette.grid;ctx.lineWidth=row===4?1.2:1;ctx.beginPath();ctx.moveTo(pad.l,yy);ctx.lineTo(w-pad.r,yy);ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign='right';ctx.fillText(String(rate),pad.l-6,yy+4);ctx.textAlign='left';ctx.fillText(String(queue),w-pad.r+6,yy+4);}}for(let tick=0;tick<=4;tick++){{const xx=pad.l+plotW*tick/4,second=Math.round(elapsed*tick/4);ctx.strokeStyle=palette.grid;ctx.beginPath();ctx.moveTo(xx,pad.t);ctx.lineTo(xx,pad.t+plotH);ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign=tick===0?'left':tick===4?'right':'center';ctx.fillText(`${{second}}s`,xx,h-8);}}ctx.fillStyle=palette.muted;ctx.textAlign='left';ctx.fillText('rate/s',pad.l,pad.t-10);ctx.textAlign='right';ctx.fillText('queue',w-pad.r,pad.t-10);if(!count)return;ctx.beginPath();queues.forEach((value,index)=>index?ctx.lineTo(x(index),queueY(value)):ctx.moveTo(x(index),queueY(value)));ctx.lineTo(x(count-1),pad.t+plotH);ctx.lineTo(x(0),pad.t+plotH);ctx.closePath();ctx.fillStyle='rgba(189,48,67,.16)';ctx.fill();ctx.strokeStyle=color;ctx.lineWidth=2.6;ctx.beginPath();rates.forEach((value,index)=>index?ctx.lineTo(x(index),rateY(value)):ctx.moveTo(x(index),rateY(value)));ctx.stroke();rates.forEach((value,index)=>{{ctx.fillStyle='#fffefa';ctx.beginPath();ctx.arc(x(index),rateY(value),3,0,Math.PI*2);ctx.fill();ctx.strokeStyle=color;ctx.lineWidth=1.8;ctx.stroke();}});}};
    draw();canvas.addEventListener('mousemove',event=>{{if(!count)return;const rect=canvas.getBoundingClientRect(),position=Math.max(0,Math.min(plotW,event.clientX-rect.left-pad.l)),index=Math.round(position/plotW*Math.max(0,count-1)),sample=snapshots[index];ctx.save();draw();ctx.strokeStyle=palette.ink;ctx.setLineDash([3,3]);ctx.beginPath();ctx.moveTo(x(index),pad.t);ctx.lineTo(x(index),pad.t+plotH);ctx.stroke();ctx.restore();tip.innerHTML=`<b>${{Math.round(sample.elapsed_seconds)}}s</b><br>rate: ${{rates[index].toFixed(1)}}/s<br>queue: ${{queues[index]}}<br>scale: 0-${{rateLimit}}/s`;tip.style.display='block';tip.style.left=`${{event.clientX+14}}px`;tip.style.top=`${{event.clientY+14}}px`;}});canvas.addEventListener('mouseleave',()=>{{tip.style.display='none';draw();}});
}}
function roomDistribution() {{ const rooms=Object.entries(traffic).map(([room,seconds])=>[room,Object.values(seconds).reduce((total,count)=>total+count,0)]).sort((a,b)=>b[1]-a[1]), peak=Math.max(1,...rooms.map(item=>item[1])); const drawProfile=()=>{{const [ctx,w,h]=context('room-profile'),pad={{l:42,r:16,t:24,b:34}},plotW=w-pad.l-pad.r,plotH=h-pad.t-pad.b,x=index=>pad.l+plotW*(rooms.length<=1?0:index/(rooms.length-1)),y=value=>pad.t+plotH*(1-value/peak);ctx.font='11px ui-monospace,Consolas,monospace';for(let row=0;row<5;row++){{const yy=pad.t+plotH*row/4,value=Math.round(peak*(4-row)/4);ctx.strokeStyle=palette.grid;ctx.beginPath();ctx.moveTo(pad.l,yy);ctx.lineTo(w-pad.r,yy);ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign='right';ctx.fillText(String(value),pad.l-6,yy+4);}}ctx.strokeStyle=palette.blue;ctx.lineWidth=2.6;ctx.beginPath();rooms.forEach((item,index)=>index?ctx.lineTo(x(index),y(item[1])):ctx.moveTo(x(index),y(item[1])));ctx.stroke();ctx.fillStyle=palette.muted;ctx.textAlign='left';ctx.fillText('busiest',pad.l,h-9);ctx.textAlign='right';ctx.fillText('quietest',w-pad.r,h-9);ctx.textAlign='left';ctx.fillText('events',pad.l,pad.t-10);}}; const drawRanking=()=>{{const [ctx,w,h]=context('room-ranking'),pad={{l:86,r:28,t:18,b:14}},top=rooms.slice(0,10),plotW=w-pad.l-pad.r,plotH=h-pad.t-pad.b;ctx.font='11px ui-monospace,Consolas,monospace';top.forEach(([room,count],index)=>{{const yy=pad.t+index*(plotH/top.length),barH=Math.max(8,plotH/top.length-5),barWidth=plotW*count/peak;ctx.fillStyle='rgba(22,119,184,.82)';ctx.fillRect(pad.l,yy,barWidth,barH);ctx.fillStyle=palette.ink;ctx.textAlign='right';ctx.fillText(room,pad.l-6,yy+barH-1);ctx.textAlign='left';ctx.fillText(String(count),pad.l+barWidth+5,yy+barH-1);}});ctx.fillStyle=palette.muted;ctx.fillText(`peak ${{peak}} events`,pad.l,pad.t-6);}}; drawProfile();drawRanking(); }}
const chartDefinitions=[['ingress','events_ingested_total','queue_depth_high','queue_depth_normal',palette.blue],['writer','sqlite_writer_batches_committed_total','sqlite_writer_queue_depth_priority','sqlite_writer_queue_depth_normal',palette.orange],['workers','worker_events_handled_total','worker_queue_depth_high','worker_queue_depth_normal',palette.green]];
function updateSummary() {{ const counters=snapshots.map(sample=>sample.counters), value=key=>Math.max(0,...counters.map(counter=>counter[key]||0)), ingressRates=snapshots.slice(1).map((sample,index)=>Math.max(0,(sample.counters.events_ingested_total-(snapshots[index].counters.events_ingested_total||0))/Math.max(.001,sample.elapsed_seconds-snapshots[index].elapsed_seconds))); const failures=value('events_persist_failed')+value('events_enqueue_failed')+value('sqlite_writer_commit_failures_total')+value('worker_handler_failures_total'); document.getElementById('kpi-rate').textContent=`${{Math.round(Math.max(0,...ingressRates))}}/s`;document.getElementById('kpi-queue').textContent=String(Math.max(value('queue_depth_high')+value('queue_depth_normal'),value('sqlite_writer_queue_depth_priority')+value('sqlite_writer_queue_depth_normal'),value('worker_queue_depth_high')+value('worker_queue_depth_normal')));document.getElementById('kpi-age').textContent=`${{value('worker_oldest_inflight_age_ms')}} ms`;document.getElementById('kpi-failures').textContent=String(failures); }}
function render() {{ const focus=document.getElementById('focus').value;chartDefinitions.forEach(definition=>{{const panel=document.getElementById(definition[0]).closest('.panel');panel.classList.toggle('is-hidden',focus!=='all'&&focus!==definition[0]);stageChart(...definition);}});updateSummary(); }}
document.getElementById('phase').addEventListener('change',event=>{{const phase=event.target.value;snapshots=phase==='all'?allSnapshots:allSnapshots.filter(sample=>sample.phase===phase);render();}});document.getElementById('focus').addEventListener('change',render);render();roomDistribution();
</script></body></html>"""


def write_dashboard_report(
    path: str,
    snapshots: list[MetricsSnapshot],
    room_traffic: dict[str, dict[int, int]],
    rate_limit: int | None = None,
) -> Path:
    """Write the load dashboard and its shared stylesheet to the requested local path."""
    report_path = Path(path)
    report_path.write_text(build_dashboard_html(snapshots, room_traffic, rate_limit), encoding="utf-8")
    stylesheet = Path(__file__).resolve().parent.parent / "assets" / "styles.css"
    (report_path.parent / "styles.css").write_text(stylesheet.read_text(encoding="utf-8"), encoding="utf-8")
    return report_path.resolve()


def build_rooms_to_watch(devices: int) -> list[str]:
    """Return the room IDs that the load test should watch.

    The generator emits events for room_000 .. room_{devices//2 - 1}, so the SSE sample must cover
    the same room population rather than a tiny subset. Watching only 10 rooms can hide the true
    delivery rate when the live path is the bottleneck.
    """
    return [f"room_{i:03d}" for i in range(min(100, max(1, devices // 2)))]


# ---------------------------------------------------------------------------
# SSE listener — one thread per room, records alarm receipt times
# ---------------------------------------------------------------------------

class RoomListener(threading.Thread):
    """Subscribes to /alarms/stream?room_id=<room> and records receipt times."""

    def __init__(self, target: str, room_id: str, received: dict[str, float], lock: threading.Lock):
        super().__init__(daemon=True)
        self.url = f"{target.rstrip('/')}/alarms/stream?room_id={room_id}"
        self.received = received  # shared: composite_key -> monotonic receipt time
        self._lock = lock
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                req = Request(self.url, headers={"Accept": "text/event-stream"})
                with urlopen(req, timeout=30) as resp:
                    if resp.status != 200:
                        print(f"  [SSE] {self.url} → HTTP {resp.status}")
                        time.sleep(1.0)
                        continue
                    lines: list[bytes] = []
                    while not self._stop.is_set():
                        line = cast(bytes, resp.readline())
                        if not line:
                            break
                        if line in {b"\n", b"\r\n"}:
                            if lines:
                                self._parse_block(b"".join(lines))
                                lines.clear()
                        else:
                            lines.append(line)
            except Exception as exc:
                if not self._stop.is_set():
                    print(f"  [SSE] {self.url} error: {exc!r}")
                    time.sleep(0.5)

    def _parse_block(self, block: bytes) -> None:
        for line in block.decode(errors="replace").splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                try:
                    obj: dict[str, Any] = json.loads(raw)
                    device_id = str(obj.get("device_id", ""))
                    room_id = str(obj.get("room_id", ""))
                    ts = str(obj.get("ts") or "")[:19]  # truncate to second for dedup key match
                    key = f"{device_id}:{room_id}:{ts}"
                    with self._lock:
                        if key not in self.received:
                            self.received[key] = time.monotonic()
                except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
                    pass

    def stop(self) -> None:
        self._stop.set()


class AlarmListener:
    """Manages per-room SSE listeners for a sample of rooms."""

    def __init__(self, target: str, rooms: list[str]):
        self._received: dict[str, float] = {}
        self._lock = threading.Lock()
        self._listeners = [RoomListener(target, r, self._received, self._lock) for r in rooms]

    def start(self) -> None:
        for l in self._listeners:
            l.start()

    def stop(self) -> None:
        for l in self._listeners:
            l.stop()

    def get_received(self) -> dict[str, float]:
        with self._lock:
            return dict(self._received)


# ---------------------------------------------------------------------------
# Worker — sends events concurrently from a shared job queue
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.planned = 0
        self.dispatched = 0
        self.sent = 0
        self.failed = 0
        self.latencies_ms: list[float] = []   # POST round-trip ms
        self.schedule_lag_ms: list[float] = []
        self._room_traffic: dict[str, dict[int, int]] = {}
        # fall_warn tracking: event_id -> send completion time (monotonic)
        self.fall_sends: dict[str, float] = {}

    def snapshot(self) -> tuple[int, int]:
        with self._lock:
            return self.sent, self.failed

    def record_dispatch(self, schedule_lag_ms: float, room_id: str = "", elapsed_seconds: float = 0.0) -> None:
        with self._lock:
            self.planned += 1
            self.dispatched += 1
            self.schedule_lag_ms.append(max(0.0, schedule_lag_ms))
            if room_id:
                room_counts = self._room_traffic.setdefault(room_id, {})
                second = max(0, int(elapsed_seconds))
                room_counts[second] = room_counts.get(second, 0) + 1

    def room_traffic_snapshot(self) -> dict[str, dict[int, int]]:
        with self._lock:
            return {room: dict(counts) for room, counts in self._room_traffic.items()}

    def record_ok(self, latency_ms: float) -> None:
        with self._lock:
            self.sent += 1
            self.latencies_ms.append(latency_ms)

    def record_fail(self) -> None:
        with self._lock:
            self.failed += 1

    def record_fall(self, event_id: str, sent_at: float) -> None:
        with self._lock:
            self.fall_sends[event_id] = sent_at


async def worker(
    job_queue: asyncio.Queue[dict[str, Any] | None],
    stats: Stats,
    session: aiohttp.ClientSession,
    target: str,
) -> None:
    """Send events through a shared keep-alive session."""
    url = target.rstrip("/") + "/events"
    while True:
        item = await job_queue.get()
        if item is None:
            job_queue.task_done()
            break
        event = item
        t0 = time.monotonic()
        try:
            async with session.post(url, json=event) as response:
                await response.read()
            elapsed_ms = (time.monotonic() - t0) * 1000
            if response.status == 202:
                stats.record_ok(elapsed_ms)
            else:
                stats.record_fail()
            if response.status == 202 and event.get("type") == "fall_warn":
                # Key matches the SSE listener's composite key (ts truncated to second)
                stats.record_fall(
                    f"{event['device_id']}:{event['room_id']}:{event['ts'][:19]}",
                    time.monotonic(),
                )
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            stats.record_fail()
        finally:
            job_queue.task_done()


# ---------------------------------------------------------------------------
# Event generation helpers
# ---------------------------------------------------------------------------

def make_event(device_id: str, room_id: str, etype: str, seq: int) -> dict[str, Any]:
    ts_unix = time.time()
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts_unix)) + \
         f".{int(ts_unix * 1000) % 1000:03d}Z"
    e: dict[str, Any] = {
        "device_id": device_id,
        "room_id": room_id,
        "type": etype,
        "ts": ts,
        "seq": seq,
    }
    if etype == "presence":
        e["in_room"] = random.choice([True, False])
    elif etype == "motion":
        e["magnitude"] = round(random.random(), 2)
    elif etype == "fall_warn":
        e["confidence"] = round(random.uniform(0.7, 0.99), 2)
    elif etype == "sleep_state":
        e["state"] = random.choice(["asleep", "awake", "unknown"])
    elif etype == "net_status":
        e["rssi"] = random.randint(-90, -50)
    return e


EVENT_TYPES = [
    "heartbeat"
] * 20 + ["motion"] * 10 + ["presence"] * 4 + ["sleep_state"] * 3 + ["net_status"] * 3 + ["fall_warn"] * 20


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _probe_baseline(target: str) -> None:
    """Send 5 sequential requests to measure single-threaded server latency."""
    url = target.rstrip("/") + "/events"
    event: dict[str, Any] = {
        "device_id": "dev_probe",
        "room_id": "room_probe",
        "type": "heartbeat",
        "ts": "",
        "seq": 0,
    }
    lats: list[float] = []
    for i in range(5):
        event["seq"] = i + 1
        event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".000Z"
        data = json.dumps(event).encode()
        req = Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
        t0 = time.monotonic()
        try:
            with urlopen(req, timeout=10) as resp:
                resp.read()
            lats.append((time.monotonic() - t0) * 1000)
        except Exception as e:
            print(f"  probe failed: {e}")
    if lats:
        print(f"Probe  : {len(lats)}/5 sequential requests ok, "
              f"avg {sum(lats)/len(lats):.0f}ms, min {min(lats):.0f}ms, max {max(lats):.0f}ms")
        if min(lats) > 500:
            print("  ⚠ Single-threaded latency > 500ms — server bottleneck, not client concurrency")


async def _capture_metrics(
    session: aiohttp.ClientSession,
    target: str,
    elapsed_seconds: float,
    phase: str,
    snapshots: list[MetricsSnapshot],
    jsonl: _Jsonl | None = None,
) -> None:
    try:
        async with session.get(f"{target.rstrip('/')}/metrics") as response:
            payload = await response.json()
        counters = payload.get("counters")
        if isinstance(counters, dict):
            snapshot: MetricsSnapshot = {
                "elapsed_seconds": elapsed_seconds,
                "phase": phase,
                "counters": {key: value for key, value in counters.items() if isinstance(value, int)},
            }
            snapshots.append(snapshot)
            if jsonl is not None:
                jsonl.emit({"type": "metrics", "snapshot": snapshot})
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        print(f"  metrics unavailable during {phase}")


async def run_load_test(args: argparse.Namespace, jsonl: _Jsonl | None = None) -> None:
    """Run a pooled asynchronous HTTP load scenario and capture server metrics."""
    jsonl = jsonl or _NullJsonl()
    connections = args.connections if args.connections is not None else args.concurrency
    if connections <= 0:
        raise ValueError("--connections must be greater than zero")
    if args.metrics_interval <= 0:
        raise ValueError("--metrics-interval must be greater than zero")

    print(f"Target : {args.target}")
    print(f"Devices: {args.devices}  Duration: {args.duration}s  "
          f"Connections: {connections}  Baseline: {args.rps} ev/dev/s")
    if args.burst:
        print("Burst  : 10x from t=30s to t=60s")

    jsonl.emit({
        "type": "started",
        "config": {
            "target": args.target,
            "devices": args.devices,
            "duration_seconds": args.duration,
            "rps": args.rps,
            "connections": connections,
            "metrics_interval_seconds": args.metrics_interval,
            "burst": bool(args.burst),
        },
    })

    # Single-threaded baseline probe so you can see server-only latency before concurrent load.
    _probe_baseline(args.target)

    # Subscribe to the same room population that the generator produces so the SSE measurement is
    # representative rather than accidentally under-sampling the traffic.
    rooms_to_watch = build_rooms_to_watch(args.devices)
    print(f"SSE    : watching {len(rooms_to_watch)} rooms")
    print()

    listener = AlarmListener(args.target, rooms_to_watch)
    listener.start()

    # A bounded pool avoids the Windows TIME_WAIT exhaustion caused by one connection per event.
    job_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=connections * 4)
    stats = Stats()
    snapshots: list[MetricsSnapshot] = []
    request_timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=connections, limit_per_host=connections)
    metrics_connector = aiohttp.TCPConnector(limit=4, limit_per_host=4)
    post_session = aiohttp.ClientSession(connector=connector, timeout=request_timeout)
    metrics_session = aiohttp.ClientSession(connector=metrics_connector, timeout=request_timeout)
    workers = [
        asyncio.create_task(worker(job_queue, stats, post_session, args.target))
        for _ in range(connections)
    ]

    # Build device list
    devices: list[dict[str, Any]] = [
        {"device_id": f"dev_{i:04d}", "room_id": f"room_{i // 2:03d}", "seq": 0}
        for i in range(args.devices)
    ]

    # Emit loop
    start = time.monotonic()
    end = start + args.duration
    next_tick = [start + random.random() / args.rps for _ in devices]  # stagger
    progress_at = start + 10.0
    metrics_at = start
    sent_snapshot = 0

    try:
        while True:
            now = time.monotonic()
            if now >= end:
                break

            elapsed = now - start
            burst_active = args.burst and 30 <= elapsed < 60
            rate = args.rps * (10.0 if burst_active else 1.0)

            for i, device in enumerate(devices):
                if now < next_tick[i]:
                    continue
                schedule_lag_ms = (now - next_tick[i]) * 1000.0
                device["seq"] += 1
                event_type = random.choice(EVENT_TYPES)
                event = make_event(device["device_id"], device["room_id"], event_type, device["seq"])
                copies = random.randint(1, 3) if event_type == "fall_warn" else 1
                for copy_index in range(copies):
                    if copy_index > 0:
                        device["seq"] += 1
                        event = dict(event, seq=device["seq"])
                    await job_queue.put(event)
                    stats.record_dispatch(schedule_lag_ms, device["room_id"], elapsed)
                next_tick[i] = now + 1.0 / rate

            if now >= metrics_at:
                phase = "burst" if burst_active else "baseline"
                await _capture_metrics(metrics_session, args.target, elapsed, phase, snapshots, jsonl)
                metrics_at += args.metrics_interval

            if now >= progress_at:
                sent_total, failed_total = stats.snapshot()
                delta = sent_total - sent_snapshot
                sent_snapshot = sent_total
                print(f"  t={elapsed:5.0f}s  sent={sent_total:7d}  "
                      f"+{delta:5d}/10s  err={failed_total}  "
                      f"{'[BURST]' if burst_active else ''}")
                jsonl.emit({
                    "type": "progress",
                    "elapsed_seconds": elapsed,
                    "phase": "burst" if burst_active else "baseline",
                    "sent": sent_total,
                    "failed": failed_total,
                })
                progress_at += 10.0

            await asyncio.sleep(0.002)

        await job_queue.join()
        await _capture_metrics(metrics_session, args.target, time.monotonic() - start, "after", snapshots, jsonl)
    finally:
        for _ in workers:
            await job_queue.put(None)
        await asyncio.gather(*workers)
        await post_session.close()
        await metrics_session.close()

    # Give SSE a moment to catch up
    print("\nWaiting 3s for SSE delivery to settle…")
    time.sleep(3)
    listener.stop()

    # Poll /alarms to verify fall_warns reached the DB, independent of SSE matching.
    try:
        with urlopen(f"{args.target}/alarms?since=0", timeout=5) as r:
            body = json.loads(r.read())
        db_alarms = body.get("alarms", [])
        print(f"  /alarms?since=0 returned {len(db_alarms)} alarms in DB")
    except Exception as exc:
        print(f"  /alarms poll failed: {exc}")
        db_alarms = []

    # ---------------------------------------------------------------------------
    # Report
    # ---------------------------------------------------------------------------
    elapsed_total = time.monotonic() - start
    received = listener.get_received()
    fall_sends = {k: v for k, v in stats.fall_sends.items()}

    # Alarm delivery latencies
    alarm_latencies_ms: list[float] = []
    for eid, sent_at in fall_sends.items():
        recv_at = received.get(eid)
        if recv_at is not None:
            alarm_latencies_ms.append((recv_at - sent_at) * 1000)

    print()
    print("=" * 52)
    print(f"  Duration         {elapsed_total:.1f}s")
    print(f"  Events planned   {stats.planned:,}")
    print(f"  Events dispatched {stats.dispatched:,}")
    print(f"  Events sent OK   {stats.sent:,}")
    print(f"  Events failed    {stats.failed:,}")
    print(f"  Throughput       {stats.sent / elapsed_total:.0f} req/s")

    if stats.schedule_lag_ms:
        schedule_lags = sorted(stats.schedule_lag_ms)
        print(f"\n  Scheduler lag (ms)")
        print(f"    p50  {statistics.median(schedule_lags):.1f}")
        print(f"    p95  {schedule_lags[int(len(schedule_lags) * 0.95)]:.1f}")
        print(f"    max  {schedule_lags[-1]:.1f}")

    if stats.latencies_ms:
        lats = sorted(stats.latencies_ms)
        print(f"\n  POST round-trip latency (ms)")
        print(f"    p50  {statistics.median(lats):.1f}")
        print(f"    p95  {lats[int(len(lats) * 0.95)]:.1f}")
        print(f"    p99  {lats[int(len(lats) * 0.99)]:.1f}")
        print(f"    max  {lats[-1]:.1f}")

    print(f"\n  Fall warns sent  {len(fall_sends):,}")
    print(f"  SSE alarms recv  {len(received):,}")
    if alarm_latencies_ms:
        al = sorted(alarm_latencies_ms)
        print(f"\n  Alarm delivery latency (send→SSE receipt, ms)")
        print(f"    p50  {statistics.median(al):.0f}   {'✓' if statistics.median(al) < 500 else '✗'}")
        p95 = al[int(len(al) * 0.95)]
        print(f"    p95  {p95:.0f}   {'✓ <1000ms' if p95 < 1000 else '✗ >1000ms  ← scoring risk'}")
        print(f"    p99  {al[int(len(al) * 0.99)]:.0f}")
        print(f"    max  {al[-1]:.0f}")
        matched_pct = len(alarm_latencies_ms) / max(len(fall_sends), 1) * 100
        print(f"    matched {matched_pct:.0f}% of fall sends to SSE events")
    elif fall_sends:
        print("  ✗ No fall_warn events matched in SSE stream — check /alarms/stream")
    else:
        print("  (no fall_warn events emitted — increase --duration or --devices)")
    metric_summary = summarize_metrics(snapshots)
    if metric_summary:
        print()
        for line in build_pressure_graph(snapshots):
            print(line)
        print("\n  Server metric deltas / peaks")
        for key in sorted(metric_summary):
            print(f"    {key}  {metric_summary[key]}")
    report_path = write_dashboard_report(
        args.report, snapshots, stats.room_traffic_snapshot(), args.chart_rate_limit
    )
    print(f"\n  Dashboard        {report_path}")
    print("=" * 52)

    alarm_p95_ms: float | None = None
    if alarm_latencies_ms:
        alarm_p95_ms = sorted(alarm_latencies_ms)[int(len(alarm_latencies_ms) * 0.95)]
    jsonl.emit({
        "type": "done",
        "summary": {
            "duration_seconds": elapsed_total,
            "planned": stats.planned,
            "dispatched": stats.dispatched,
            "sent": stats.sent,
            "failed": stats.failed,
            "throughput_rps": (stats.sent / elapsed_total) if elapsed_total else 0.0,
            "fall_warns_sent": len(fall_sends),
            "sse_alarms_received": len(received),
            "alarm_latency_p95_ms": alarm_p95_ms,
        },
        "report_path": str(report_path),
    })


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default=TARGET)
    parser.add_argument("--devices", type=int, default=500)
    parser.add_argument("--duration", type=float, default=90.0)
    parser.add_argument("--rps", type=float, default=1.0, help="Events per device per second (baseline)")
    parser.add_argument("--concurrency", type=int, default=64, help="Legacy alias for --connections")
    parser.add_argument("--connections", type=int, default=None, help="Maximum reusable POST connections")
    parser.add_argument("--metrics-interval", type=float, default=1.0, help="Seconds between metric snapshots")
    parser.add_argument("--report", default="loadtest-dashboard.html", help="Path for the HTML dashboard report")
    parser.add_argument(
        "--chart-rate-limit", type=int, default=None,
        help="Fixed rate/s ceiling shared by dashboard charts; default uses rounded headroom",
    )
    parser.add_argument("--burst", action="store_true", help="Do a 10x burst from t=30s to t=60s")
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Emit machine-readable JSON-lines progress to stdout for a controlling process",
    )
    args = parser.parse_args()
    jsonl = _Jsonl(args.jsonl)
    if args.jsonl:
        # A controlling process reads stdout as a pipe, which on Windows defaults to cp1252 and
        # crashes on the report's →, ✓, ⚠, … glyphs. Force UTF-8 so both the JSON-lines stream
        # and the human-readable report encode cleanly for the parent.
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError):
                pass
    try:
        asyncio.run(run_load_test(args, jsonl))
    except Exception as exc:
        jsonl.emit({"type": "error", "message": str(exc)})
        raise


if __name__ == "__main__":
    main()
