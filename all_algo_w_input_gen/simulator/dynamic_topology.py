from __future__ import annotations
import csv, random
from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass
class DynamicWindow:
    scenario_id:int; window_id:int; start_time:float; end_time:float
    num_nodes:int; num_edges:int; bandwidth:float; latency:float; failure_rate:float; loss_rate:float

def _clamp_edges(n:int,e:int)->int:
    return max(n-1, min(n*(n-1)//2, e))

def generate_dynamic_windows(scenario_id:int,num_nodes:int,num_edges:int,bandwidth:float,latency:float,failure_rate:float,loss_rate:float,total_time:float=5000.0,check_interval:float=1000.0,seed:int=42)->list[DynamicWindow]:
    rng=random.Random(seed+scenario_id*100000); min_e=num_nodes-1; max_e=num_nodes*(num_nodes-1)//2
    EDGE_FACTORS=[1.00,0.35,0.75,0.15,0.95]
    BANDWIDTH_FACTORS=[1.00,0.20,2.00,0.50,5.00]
    LATENCY_FACTORS=[1.00,4.00,0.50,8.00,2.00]
    FAILURE_FACTORS=[1.00,3.00,0.50,6.00,1.50]
    LOSS_FACTORS=[1.00,2.00,0.00,3.00,1.50]
    windows=[]; count=int(total_time//check_interval)
    for wid in range(count):
        if wid==0: e=num_edges
        else:
            e=int(min_e+EDGE_FACTORS[wid%len(EDGE_FACTORS)]*(max_e-min_e))
            e+=rng.randint(-max(1,num_nodes//3),max(1,num_nodes//3))
        windows.append(DynamicWindow(scenario_id,wid,wid*check_interval,(wid+1)*check_interval,num_nodes,_clamp_edges(num_nodes,e),max(.1,bandwidth*BANDWIDTH_FACTORS[wid%5]),max(1.0,latency*LATENCY_FACTORS[wid%5]),max(.0001,failure_rate*FAILURE_FACTORS[wid%5]),min(.30,max(0.0,loss_rate*LOSS_FACTORS[wid%5]))))
    return windows

def read_initial_inputs(path:str)->list[dict]:
    rows=[]
    with open(path,newline='') as f:
        reader=csv.DictReader(f); req=['num_nodes','num_edges','bandwidth','latency','failure_rate','loss_rate']
        miss=[c for c in req if c not in (reader.fieldnames or [])]
        if miss: raise ValueError(f'Missing required columns: {miss}')
        for i,row in enumerate(reader):
            rows.append(dict(scenario_id=i,num_nodes=int(row['num_nodes']),num_edges=int(row['num_edges']),bandwidth=float(row['bandwidth']),latency=float(row['latency']),failure_rate=float(row['failure_rate']),loss_rate=float(row['loss_rate']),total_time=float(row.get('total_time',5000) or 5000),check_interval=float(row.get('check_interval',1000) or 1000)))
    return rows

def write_windows_csv(windows:list[DynamicWindow],output:str)->None:
    p=Path(output); p.parent.mkdir(parents=True,exist_ok=True); fields=list(DynamicWindow.__dataclass_fields__.keys())
    with open(p,'w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); [w.writerow(asdict(x)) for x in windows]

def read_windows_csv(path:str)->list[DynamicWindow]:
    out=[]
    with open(path,newline='') as f:
        for r in csv.DictReader(f):
            out.append(DynamicWindow(int(r['scenario_id']),int(r['window_id']),float(r['start_time']),float(r['end_time']),int(r['num_nodes']),int(r['num_edges']),float(r['bandwidth']),float(r['latency']),float(r['failure_rate']),float(r['loss_rate'])))
    return out
