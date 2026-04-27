const { useState } = React;

const smoothPath = (pts, tension=0.28) => {
  if (!pts||pts.length<2) return '';
  let d=`M ${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`;
  for (let i=0;i<pts.length-1;i++) {
    const p0=pts[Math.max(0,i-1)],p1=pts[i],p2=pts[i+1],p3=pts[Math.min(pts.length-1,i+2)];
    const cp1x=p1[0]+(p2[0]-p0[0])*tension,cp1y=p1[1]+(p2[1]-p0[1])*tension;
    const cp2x=p2[0]-(p3[0]-p1[0])*tension,cp2y=p2[1]-(p3[1]-p1[1])*tension;
    d+=` C ${cp1x.toFixed(1)},${cp1y.toFixed(1)} ${cp2x.toFixed(1)},${cp2y.toFixed(1)} ${p2[0].toFixed(1)},${p2[1].toFixed(1)}`;
  }
  return d;
};

// ── Harvest Line Chart ─────────────────────────────────────
const HarvestChart = ({ data, compact=false }) => {
  const t = useTheme();
  const W=520, H=compact?130:180;
  const pad={top:20,right:20,bottom:30,left:44};
  const cW=W-pad.left-pad.right, cH=H-pad.top-pad.bottom;
  const maxV=Math.max(...data.flatMap(d=>[d.real||0,d.proj]))*1.15;
  const toX=i=>pad.left+(i/(data.length-1))*cW;
  const toY=v=>pad.top+cH-(v/maxV)*cH;
  const realPts=data.map((d,i)=>d.real!=null?[toX(i),toY(d.real)]:null).filter(Boolean);
  const projPts=data.map((d,i)=>[toX(i),toY(d.proj)]);
  const splitIdx=data.findIndex(d=>d.real==null);
  const areaClose=realPts.length>1?` L ${realPts[realPts.length-1][0].toFixed(1)},${(pad.top+cH).toFixed(1)} L ${realPts[0][0].toFixed(1)},${(pad.top+cH).toFixed(1)} Z`:'';
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{overflow:'visible'}}>
      <defs>
        <linearGradient id="ag" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={t.accent} stopOpacity="0.22"/>
          <stop offset="100%" stopColor={t.accent} stopOpacity="0.02"/>
        </linearGradient>
      </defs>
      {[0,1,2,3,4].map(i=>{
        const y=pad.top+(i/4)*cH;
        const v=Math.round(maxV*(1-i/4));
        return <g key={i}>
          <line x1={pad.left} y1={y} x2={pad.left+cW} y2={y} stroke={t.divider} strokeWidth={1} strokeDasharray="3,5"/>
          <text x={pad.left-8} y={y+4} textAnchor="end" fontSize={9} fill={t.textLight} fontFamily="DM Mono,monospace">{v>0?v:''}</text>
        </g>;
      })}
      <line x1={pad.left} y1={pad.top+cH} x2={pad.left+cW} y2={pad.top+cH} stroke={t.divider} strokeWidth={1}/>
      {projPts.length>1&&<path d={smoothPath(projPts)+` L ${projPts[projPts.length-1][0]},${pad.top+cH} L ${projPts[0][0]},${pad.top+cH} Z`} fill={t.accent} fillOpacity={0.04}/>}
      {splitIdx>0&&<line x1={toX(splitIdx)} y1={pad.top} x2={toX(splitIdx)} y2={pad.top+cH} stroke={t.accent} strokeWidth={1} strokeDasharray="4,4" strokeOpacity={0.35}/>}
      {splitIdx>0&&<text x={toX(splitIdx)+5} y={pad.top+10} fontSize={8} fill={t.accentText} fontFamily="Plus Jakarta Sans">Proyectado →</text>}
      <path d={smoothPath(projPts)} fill="none" stroke={t.accent} strokeWidth={1.8} strokeDasharray="5,4" strokeOpacity={0.5}/>
      {realPts.length>1&&<path d={smoothPath(realPts)+areaClose} fill="url(#ag)"/>}
      {realPts.length>1&&<path d={smoothPath(realPts)} fill="none" stroke={t.accent} strokeWidth={2.8}/>}
      {realPts.map(([x,y],i)=><g key={i}>
        <circle cx={x} cy={y} r={9} fill={t.accent} fillOpacity={0.12}/>
        <circle cx={x} cy={y} r={5} fill={t.accent} stroke="#fff" strokeWidth={2}/>
        <text x={x} y={y-12} textAnchor="middle" fontSize={9} fill={t.accentText} fontFamily="DM Mono,monospace" fontWeight="600">{data[i].real}</text>
      </g>)}
      {data.map((d,i)=>d.real==null&&<circle key={i} cx={toX(i)} cy={toY(d.proj)} r={4} fill="none" stroke={t.accent} strokeWidth={1.8} strokeOpacity={0.55}/>)}
      {data.map((d,i)=><text key={i} x={toX(i)} y={H-6} textAnchor="middle" fontSize={9}
        fill={t.textMuted} fontFamily="Plus Jakarta Sans" fontWeight={d.real==null?'400':'600'}>{d.mes}</text>)}
    </svg>
  );
};

// ── Donut Chart ────────────────────────────────────────────
const DonutChart = ({ data, compact=false }) => {
  const t = useTheme();
  const sz=compact?120:150, cx=sz/2, cy=sz/2, R=compact?46:58, ri=compact?30:38;
  const colors=['#2a7d46','#3a9a5c','#5ab878','#8dd4a8','#c2e8cf'];
  const total=data.reduce((s,d)=>s+d.pct,0);
  const [hov,setHov]=useState(null);
  let angle=-Math.PI/2;
  const slices=data.map((d,i)=>{
    const sweep=(d.pct/total)*2*Math.PI;
    const sa=angle,ea=angle+sweep;
    angle+=sweep;
    return {...d,sa,ea,mid:sa+sweep/2,i};
  });
  const arcPath=(sa,ea,ro,ri_)=>{
    const x1=cx+ro*Math.cos(sa),y1=cy+ro*Math.sin(sa);
    const x2=cx+ro*Math.cos(ea),y2=cy+ro*Math.sin(ea);
    const xi1=cx+ri_*Math.cos(ea),yi1=cy+ri_*Math.sin(ea);
    const xi2=cx+ri_*Math.cos(sa),yi2=cy+ri_*Math.sin(sa);
    return `M ${x1} ${y1} A ${ro} ${ro} 0 ${ea-sa>Math.PI?1:0} 1 ${x2} ${y2} L ${xi1} ${yi1} A ${ri_} ${ri_} 0 ${ea-sa>Math.PI?1:0} 0 ${xi2} ${yi2} Z`;
  };
  return (
    <div style={{display:'flex',alignItems:'center',gap:compact?12:20}}>
      <svg width={sz} height={sz} style={{flexShrink:0}}>
        {slices.map(s=>{
          const ox=Math.cos(s.mid)*(hov===s.i?3:0),oy=Math.sin(s.mid)*(hov===s.i?3:0);
          return <path key={s.i} d={arcPath(s.sa,s.ea,R,ri)} fill={colors[s.i]} opacity={hov===s.i?1:0.88}
            transform={`translate(${ox.toFixed(1)},${oy.toFixed(1)})`}
            onMouseEnter={()=>setHov(s.i)} onMouseLeave={()=>setHov(null)}
            style={{cursor:'pointer',transition:'opacity .15s,transform .15s'}}/>;
        })}
        <circle cx={cx} cy={cy} r={ri-2} fill={t.card}/>
        {hov!==null
          ?<>
            <text x={cx} y={cy-3} textAnchor="middle" fontSize={compact?13:16} fontWeight="800" fill={t.text} fontFamily="Plus Jakarta Sans">{data[hov].pct}%</text>
            <text x={cx} y={cy+11} textAnchor="middle" fontSize={compact?7:8} fill={t.textMuted} fontFamily="Plus Jakarta Sans">{data[hov].cal}</text>
          </>
          :<>
            <text x={cx} y={cy-3} textAnchor="middle" fontSize={compact?10:11} fontWeight="700" fill={t.text} fontFamily="Plus Jakarta Sans">4,820</text>
            <text x={cx} y={cy+11} textAnchor="middle" fontSize={compact?7:8} fill={t.textMuted} fontFamily="Plus Jakarta Sans">TM total</text>
          </>
        }
      </svg>
      <div style={{flex:1}}>
        {data.map((d,i)=>(
          <div key={d.cal} onMouseEnter={()=>setHov(i)} onMouseLeave={()=>setHov(null)}
            style={{display:'flex',alignItems:'center',gap:6,marginBottom:compact?4:6,cursor:'default',
              opacity:hov===null||hov===i?1:0.4,transition:'opacity .15s'}}>
            <span style={{width:8,height:8,borderRadius:2,background:colors[i],flexShrink:0,display:'inline-block'}}/>
            <span style={{fontSize:compact?10:11,color:t.textMuted,flex:1}}>{d.cal}</span>
            <span style={{fontSize:compact?10:11,fontWeight:700,color:t.text,fontFamily:"'DM Mono',monospace"}}>{d.pct}%</span>
          </div>
        ))}
      </div>
    </div>
  );
};

// ── Fundo Bar Chart ────────────────────────────────────────
const FundoBarChart = () => {
  const t = useTheme();
  const items=[{name:'El Palomar',real:2640,proj:2850},{name:'La Victoria',real:1710,proj:1820},{name:'Los Pinos',real:0,proj:150}];
  const maxV=Math.max(...items.flatMap(f=>[f.real,f.proj]))*1.1;
  const W=340,H=110,padL=80,padB=22,padT=12;
  const cW=W-padL-10,cH=H-padB-padT,barH=18,gap=14;
  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{overflow:'visible'}}>
      {[0,1,2,3].map(i=>{
        const x=padL+(i/3)*cW,v=Math.round(maxV*i/3);
        return <g key={i}>
          <line x1={x} y1={padT} x2={x} y2={padT+cH} stroke={t.divider} strokeWidth={1} strokeDasharray="2,4"/>
          <text x={x} y={H-6} textAnchor="middle" fontSize={8} fill={t.textLight} fontFamily="DM Mono,monospace">{v}</text>
        </g>;
      })}
      {items.map((f,i)=>{
        const yBase=padT+i*(barH*2+gap);
        const wReal=(f.real/maxV)*cW,wProj=(f.proj/maxV)*cW;
        return <g key={f.name}>
          <text x={padL-8} y={yBase+barH-3} textAnchor="end" fontSize={9} fill={t.textMuted} fontFamily="Plus Jakarta Sans">{f.name}</text>
          <rect x={padL} y={yBase} width={wProj} height={barH} rx={3} fill={t.accent} fillOpacity={0.22}/>
          <text x={padL+wProj+4} y={yBase+barH-3} fontSize={8} fill={t.textMuted} fontFamily="DM Mono,monospace">{f.proj}</text>
          {f.real>0&&<>
            <rect x={padL} y={yBase+barH+2} width={wReal} height={barH-4} rx={3} fill={t.accent} fillOpacity={0.85}/>
            <text x={padL+wReal+4} y={yBase+barH*2-3} fontSize={8} fill={t.accentText} fontFamily="DM Mono,monospace" fontWeight="600">{f.real}</text>
          </>}
        </g>;
      })}
      <g transform={`translate(${padL},${H-4})`}>
        <rect width={8} height={6} rx={1} fill={t.accent} fillOpacity={0.22}/>
        <text x={11} y={6} fontSize={7.5} fill={t.textMuted} fontFamily="Plus Jakarta Sans">Proyectado</text>
        <rect x={68} width={8} height={6} rx={1} fill={t.accent}/>
        <text x={79} y={6} fontSize={7.5} fill={t.accentText} fontFamily="Plus Jakarta Sans">Real</text>
      </g>
    </svg>
  );
};

// ── KPI Card ───────────────────────────────────────────────
const KpiCard = ({ label, value, change, trend, icon, color, big=false }) => {
  const t = useTheme();
  return (
    <Card style={{flex:1,minWidth:0}}>
      <div style={{display:'flex',alignItems:'flex-start',justifyContent:'space-between',marginBottom:14}}>
        <div style={{width:big?46:38,height:big?46:38,borderRadius:10,background:color+'22',
          display:'flex',alignItems:'center',justifyContent:'center'}}>
          <Icon name={icon} size={big?22:18} color={color} sw={2}/>
        </div>
        <span style={{fontSize:11,fontWeight:600,color:trend==='up'?t.ok:t.warn,
          background:trend==='up'?t.okLight:t.warnLight,padding:'3px 8px',borderRadius:20}}>{change}</span>
      </div>
      <div style={{fontSize:big?34:27,fontWeight:800,color:t.text,letterSpacing:'-0.5px',lineHeight:1}}>{value}</div>
      <div style={{fontSize:12,color:t.textMuted,marginTop:5,fontWeight:500}}>{label}</div>
    </Card>
  );
};

const HealthRow = ({ items }) => {
  const t = useTheme();
  return (
    <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:12}}>
      {items.map(h=>(
        <Card key={h.key} p="14px">
          <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:6}}>
            <StatusDot status={h.status}/>
            <span style={{fontSize:12,fontWeight:600,color:t.text}}>{h.name}</span>
          </div>
          <div style={{fontSize:11,color:t.textMuted}}>{h.detail}</div>
          <div style={{fontSize:10,color:t.textLight,marginTop:3}}>Verificado {h.check}</div>
        </Card>
      ))}
    </div>
  );
};

// ── Analyst Dashboard ──────────────────────────────────────
const Dashboard = () => {
  const t = useTheme();
  const { filters } = useFilters();
  const cosechaData = MOCK.cosecha[filters.fundo] || MOCK.cosecha.todos;
  const fundoLabel = filters.fundo==='todos' ? 'Todos los fundos'
    : MOCK.fundos.find(f=>f.id===filters.fundo)?.name || '';

  return (
    <div className="fade-in" style={{display:'flex',flexDirection:'column',gap:20}}>
      {/* Filter bar */}
      <FilterBar config={{fundo:true}} />

      {/* KPIs */}
      <div style={{display:'flex',gap:14}}>
        <KpiCard label="Cosecha Proyectada (TM)" value="4,820" change="+8.3%" trend="up" icon="trendUp" color={t.accent}/>
        <KpiCard label="Eficiencia ETL" value="99.2%" change="+0.1%" trend="up" icon="activity" color={t.ok}/>
        <KpiCard label="Alertas Activas" value="15" change="-3 hoy" trend="up" icon="alertC" color={t.warn}/>
        <KpiCard label="Lotes Activos" value="127" change="+4" trend="up" icon="map" color={t.accent}/>
      </div>

      {/* Health */}
      <div>
        <div style={{fontSize:11,fontWeight:700,color:t.textMuted,textTransform:'uppercase',letterSpacing:'0.8px',marginBottom:10}}>Estado del Sistema</div>
        <HealthRow items={MOCK.health}/>
      </div>

      {/* Main chart */}
      <Card>
        <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:12}}>
          <div>
            <div style={{fontSize:15,fontWeight:700,color:t.text}}>Proyección de Cosecha — Temporada 2025–2026</div>
            <div style={{fontSize:11,color:t.textMuted,marginTop:3}}>
              TM · Arándano · {fundoLabel}
            </div>
          </div>
          <div style={{display:'flex',gap:16,fontSize:11,color:t.textMuted,alignItems:'center'}}>
            <span style={{display:'flex',alignItems:'center',gap:5}}>
              <span style={{width:16,height:3,background:t.accent,borderRadius:2,display:'inline-block'}}/>Real
            </span>
            <span style={{display:'flex',alignItems:'center',gap:5}}>
              <svg width="16" height="3"><line x1="0" y1="1.5" x2="16" y2="1.5" stroke={t.accent} strokeWidth="2" strokeDasharray="4,3" strokeOpacity="0.6"/></svg>
              Proyectado
            </span>
          </div>
        </div>
        <HarvestChart data={cosechaData}/>
      </Card>

      {/* Bottom row */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:14}}>
        <Card>
          <div style={{fontSize:14,fontWeight:700,color:t.text,marginBottom:4}}>Calibres (Modelo IA)</div>
          <div style={{fontSize:11,color:t.textMuted,marginBottom:14}}>Distribución proyectada · Biloxi</div>
          <DonutChart data={MOCK.calibres}/>
        </Card>
        <Card>
          <div style={{fontSize:14,fontWeight:700,color:t.text,marginBottom:4}}>Cosecha por Fundo</div>
          <div style={{fontSize:11,color:t.textMuted,marginBottom:12}}>TM · Real vs Proyectado</div>
          <FundoBarChart/>
        </Card>
        <div style={{display:'flex',flexDirection:'column',gap:12}}>
          <Card style={{flex:1}}>
            <div style={{fontSize:13,fontWeight:700,color:t.text,marginBottom:10}}>Resumen</div>
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:8}}>
              {[
                {l:'Fundos',v:'2 activos',icon:'map',c:t.accent},
                {l:'Hectáreas',v:'1,015 ha',icon:'bar',c:t.ok},
                {l:'Variedades',v:'6 tipos',icon:'pkg',c:t.warn},
                {l:'Duplicados',v:'5 pend.',icon:'shuffle',c:t.err},
              ].map(s=>(
                <div key={s.l} style={{background:t.accentLight,borderRadius:8,padding:'9px 10px',
                  display:'flex',alignItems:'center',gap:7}}>
                  <Icon name={s.icon} size={14} color={s.c} sw={2}/>
                  <div>
                    <div style={{fontSize:13,fontWeight:800,color:t.text}}>{s.v}</div>
                    <div style={{fontSize:9,color:t.textMuted}}>{s.l}</div>
                  </div>
                </div>
              ))}
            </div>
          </Card>
          <Card p="14px">
            <div style={{fontSize:12,fontWeight:700,color:t.text,marginBottom:8}}>Última ejecución ETL</div>
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
              <div>
                <div style={{fontSize:16,fontWeight:800,color:t.accent,fontFamily:"'DM Mono',monospace"}}>ETL-2847</div>
                <div style={{fontSize:10,color:t.textMuted}}>Hace 2 horas · 4m 12s</div>
              </div>
              <Badge status="ok"/>
            </div>
            <div style={{marginTop:8,fontSize:11,color:t.textMuted}}>
              <span style={{fontFamily:"'DM Mono',monospace",fontWeight:600,color:t.text}}>2.84M</span> registros procesados
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
};

// ── Executive Portal ───────────────────────────────────────
const ExecutivePortal = () => {
  const t = useTheme();
  const exportDest = [
    {country:'Estados Unidos',pct:45,flag:'🇺🇸'},
    {country:'Países Bajos',pct:20,flag:'🇳🇱'},
    {country:'China',pct:20,flag:'🇨🇳'},
    {country:'Reino Unido',pct:15,flag:'🇬🇧'},
  ];
  const fundoCards = [
    {name:'El Palomar',region:'Lambayeque',ha:480,tm:2850,real:2640,status:'ok',pct:92.6},
    {name:'La Victoria',region:'Piura',ha:320,tm:1820,real:1710,status:'ok',pct:93.9},
    {name:'Los Pinos',region:'La Libertad',ha:215,tm:150,real:0,status:'warning',pct:0},
  ];

  return (
    <div className="fade-in" style={{display:'flex',flexDirection:'column',gap:24}}>
      {/* Hero header */}
      <div style={{
        background:`linear-gradient(135deg, ${t.accent}, ${t.accentHov})`,
        borderRadius:16, padding:'28px 32px',
        boxShadow:`0 8px 32px ${t.accent}44`,
        display:'flex',alignItems:'center',justifyContent:'space-between',
      }}>
        <div>
          <div style={{fontSize:12,fontWeight:700,color:'rgba(255,255,255,0.7)',letterSpacing:'1px',
            textTransform:'uppercase',marginBottom:8}}>Temporada 2025 – 2026 · Arándano</div>
          <div style={{fontSize:52,fontWeight:800,color:'#fff',letterSpacing:'-2px',lineHeight:1}}>4,820 TM</div>
          <div style={{fontSize:16,color:'rgba(255,255,255,0.85)',marginTop:8,fontWeight:500}}>
            Proyección total de cosecha para exportación
          </div>
        </div>
        <div style={{textAlign:'right'}}>
          <div style={{display:'flex',gap:14}}>
            {[
              {l:'Avance temporada',v:'79%'},
              {l:'Calibre premium',v:'40%'},
              {l:'Fundos activos',v:'2/3'},
            ].map(s=>(
              <div key={s.l} style={{background:'rgba(255,255,255,0.15)',borderRadius:12,
                padding:'14px 18px',backdropFilter:'blur(10px)',textAlign:'center',minWidth:110}}>
                <div style={{fontSize:28,fontWeight:800,color:'#fff'}}>{s.v}</div>
                <div style={{fontSize:11,color:'rgba(255,255,255,0.75)',marginTop:4}}>{s.l}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* KPI row */}
      <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:14}}>
        {[
          {l:'Cosecha Real YTD',v:'5,070 TM',sub:'Oct–Mar acumulado',icon:'bar',c:t.accent},
          {l:'Cumplimiento',v:'93.0%',sub:'vs proyección inicial',icon:'checkC',c:t.ok},
          {l:'Calibres ≥14mm',v:'75%',sub:'Alta demanda export',icon:'star',c:t.warn},
          {l:'ETL Uptime',v:'99.2%',sub:'Último mes',icon:'activity',c:t.ok},
        ].map(s=>(
          <Card key={s.l} style={{textAlign:'center'}}>
            <div style={{width:44,height:44,borderRadius:12,background:s.c+'22',
              display:'flex',alignItems:'center',justifyContent:'center',margin:'0 auto 12px'}}>
              <Icon name={s.icon} size={22} color={s.c} sw={2}/>
            </div>
            <div style={{fontSize:30,fontWeight:800,color:t.text,letterSpacing:'-0.5px'}}>{s.v}</div>
            <div style={{fontSize:12,fontWeight:600,color:t.text,marginTop:4}}>{s.l}</div>
            <div style={{fontSize:11,color:t.textMuted,marginTop:2}}>{s.sub}</div>
          </Card>
        ))}
      </div>

      {/* Main chart + calibres */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 340px',gap:16}}>
        <Card>
          <div style={{fontSize:16,fontWeight:700,color:t.text,marginBottom:4}}>Curva de Cosecha — Temporada 2025–2026</div>
          <div style={{fontSize:12,color:t.textMuted,marginBottom:16}}>TM mensual · Real (line) vs Proyectado (dash)</div>
          <HarvestChart data={MOCK.cosecha.todos} compact/>
        </Card>
        <Card>
          <div style={{fontSize:15,fontWeight:700,color:t.text,marginBottom:4}}>Distribución de Calibres</div>
          <div style={{fontSize:12,color:t.textMuted,marginBottom:16}}>Proyección IA · Biloxi · El Palomar</div>
          <DonutChart data={MOCK.calibres} compact/>
          <div style={{marginTop:16,paddingTop:14,borderTop:`1px solid ${t.divider}`}}>
            <div style={{fontSize:11,color:t.textMuted,fontWeight:600,marginBottom:8}}>Calibres premium (≥14mm)</div>
            <div style={{display:'flex',alignItems:'center',gap:10}}>
              <div style={{flex:1,height:10,background:t.divider,borderRadius:5,overflow:'hidden'}}>
                <div style={{width:'75%',height:'100%',background:`linear-gradient(90deg, ${t.accent}, ${t.accentHov})`,borderRadius:5}}/>
              </div>
              <span style={{fontSize:16,fontWeight:800,color:t.accent}}>75%</span>
            </div>
          </div>
        </Card>
      </div>

      {/* Fundo cards + export */}
      <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr 280px',gap:14}}>
        {fundoCards.map(f=>(
          <Card key={f.name}>
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:12}}>
              <div>
                <div style={{fontSize:15,fontWeight:700,color:t.text}}>{f.name}</div>
                <div style={{fontSize:11,color:t.textMuted}}>{f.region} · {f.ha} ha</div>
              </div>
              <Badge status={f.status}/>
            </div>
            <div style={{fontSize:30,fontWeight:800,color:t.text,letterSpacing:'-0.5px'}}>{f.tm} TM</div>
            <div style={{fontSize:11,color:t.textMuted,marginTop:2}}>Proyección temporada</div>
            {f.real>0&&<>
              <div style={{marginTop:12,height:6,background:t.divider,borderRadius:3,overflow:'hidden'}}>
                <div style={{width:`${f.pct}%`,height:'100%',background:t.accent,borderRadius:3}}/>
              </div>
              <div style={{fontSize:10,color:t.textMuted,marginTop:4}}>Real: {f.real} TM ({f.pct}% cumplido)</div>
            </>}
            {f.real===0&&<div style={{fontSize:11,color:t.warn,marginTop:8,fontWeight:500}}>Pendiente de inicio</div>}
          </Card>
        ))}
        {/* Export destinations */}
        <Card>
          <div style={{fontSize:13,fontWeight:700,color:t.text,marginBottom:14,display:'flex',alignItems:'center',gap:7}}>
            <Icon name="globe" size={15} color={t.accent} sw={2}/>
            Destinos de Exportación
          </div>
          {exportDest.map(d=>(
            <div key={d.country} style={{marginBottom:10}}>
              <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
                <span style={{fontSize:12,color:t.text,fontWeight:500}}>{d.flag} {d.country}</span>
                <span style={{fontSize:12,fontWeight:700,color:t.accentText}}>{d.pct}%</span>
              </div>
              <div style={{height:5,background:t.divider,borderRadius:3,overflow:'hidden'}}>
                <div style={{width:`${d.pct}%`,height:'100%',background:t.accent,borderRadius:3}}/>
              </div>
            </div>
          ))}
        </Card>
      </div>
    </div>
  );
};

Object.assign(window, { Dashboard, ExecutivePortal, HarvestChart, DonutChart });
