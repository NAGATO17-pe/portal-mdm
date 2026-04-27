const { createContext, useContext, useState: useStateData } = React;

const ThemeCtx = createContext(null);
const useTheme = () => useContext(ThemeCtx);

// ── Filters Context ────────────────────────────────────────
const FiltersCtx = createContext(null);
const useFilters = () => useContext(FiltersCtx);
const FiltersProvider = ({ children }) => {
  const [filters, setFilters] = useStateData({
    fundo: 'todos', variedad: 'todas', estado: 'todos',
    rol: 'todos', tabla: 'todas', rango: 'todo', tipo: 'todos', q: '',
  });
  const setFilter = (key, val) => setFilters(f => ({ ...f, [key]: val }));
  const resetFilters = () => setFilters({ fundo:'todos', variedad:'todas', estado:'todos', rol:'todos', tabla:'todas', rango:'todo', tipo:'todos', q:'' });
  const activeCount = Object.entries(filters).filter(([k,v]) =>
    v !== 'todos' && v !== 'todas' && v !== 'todo' && v !== '').length;
  return <FiltersCtx.Provider value={{ filters, setFilter, resetFilters, activeCount }}>{children}</FiltersCtx.Provider>;
};

const THEMES = {
  verdeGlass: {
    id:'verdeGlass', name:'Verde Agro',
    bodyBg:'linear-gradient(140deg, #dff0e3 0%, #e6f0f8 55%, #eceef7 100%)',
    orbs:'radial-gradient(ellipse 60% 50% at 15% 40%, rgba(42,125,70,0.1) 0%, transparent 70%), radial-gradient(ellipse 50% 40% at 85% 20%, rgba(30,100,160,0.07) 0%, transparent 60%), radial-gradient(ellipse 40% 50% at 60% 85%, rgba(80,180,120,0.07) 0%, transparent 55%)',
    sidebar:'rgba(255,255,255,0.75)', sidebarBorder:'rgba(255,255,255,0.92)',
    topbar:'rgba(255,255,255,0.85)', topbarBorder:'rgba(42,125,70,0.1)',
    card:'rgba(255,255,255,0.72)', cardBorder:'rgba(255,255,255,0.9)', blur:'blur(22px)',
    accent:'#2a7d46', accentHov:'#1f6337', accentLight:'#e8f5ec', accentText:'#1a5e32',
    text:'#1a2e1e', textMuted:'#5a7062', textLight:'#9ab0a0',
    warn:'#a07010', warnLight:'#fdf7e2', err:'#b83030', errLight:'#fdecea',
    ok:'#1e8048', okLight:'#eaf7ef',
    shadow:'0 4px 28px rgba(30,70,45,0.09), inset 0 1px 0 rgba(255,255,255,0.8)',
    shadowHov:'0 8px 36px rgba(30,70,45,0.15)',
    divider:'rgba(42,125,70,0.1)', rowHov:'rgba(42,125,70,0.04)',
    navActive:'rgba(42,125,70,0.12)', navActiveBorder:'#2a7d46',
    btnPrimary:'#2a7d46', btnPrimaryText:'#fff',
    tag:'#e8f5ec', tagText:'#1a5e32',
  },
  ejecutivoDark: {
    id:'ejecutivoDark', name:'Ejecutivo',
    bodyBg:'linear-gradient(140deg, #0b1a10 0%, #0e1920 55%, #0c1220 100%)',
    orbs:'radial-gradient(ellipse 60% 50% at 15% 40%, rgba(42,150,80,0.12) 0%, transparent 70%), radial-gradient(ellipse 50% 40% at 85% 20%, rgba(30,80,150,0.1) 0%, transparent 60%)',
    sidebar:'rgba(15,38,22,0.85)', sidebarBorder:'rgba(255,255,255,0.07)',
    topbar:'rgba(10,28,16,0.9)', topbarBorder:'rgba(255,255,255,0.06)',
    card:'rgba(255,255,255,0.055)', cardBorder:'rgba(255,255,255,0.1)', blur:'blur(22px)',
    accent:'#38c06a', accentHov:'#2da85c', accentLight:'rgba(56,192,106,0.14)', accentText:'#5cd882',
    text:'#dceadf', textMuted:'#7a9a82', textLight:'#3a5642',
    warn:'#d4a843', warnLight:'rgba(212,168,67,0.14)', err:'#e05c5c', errLight:'rgba(224,92,92,0.14)',
    ok:'#38c06a', okLight:'rgba(56,192,106,0.14)',
    shadow:'0 4px 28px rgba(0,0,0,0.32), inset 0 1px 0 rgba(255,255,255,0.06)',
    shadowHov:'0 8px 40px rgba(0,0,0,0.44)',
    divider:'rgba(255,255,255,0.07)', rowHov:'rgba(255,255,255,0.04)',
    navActive:'rgba(56,192,106,0.14)', navActiveBorder:'#38c06a',
    btnPrimary:'#38c06a', btnPrimaryText:'#0b1a10',
    tag:'rgba(56,192,106,0.14)', tagText:'#5cd882',
  },
  analitico: {
    id:'analitico', name:'Analítico',
    bodyBg:'#f2f4f7', orbs:'none',
    sidebar:'#ffffff', sidebarBorder:'#e0e6ea',
    topbar:'#ffffff', topbarBorder:'#e0e6ea',
    card:'#ffffff', cardBorder:'#e0e6ea', blur:'none',
    accent:'#1d6e38', accentHov:'#155a2e', accentLight:'#eaf5ee', accentText:'#1d6e38',
    text:'#1a2633', textMuted:'#5a6875', textLight:'#9baab2',
    warn:'#8c6600', warnLight:'#fdf7e0', err:'#b02828', errLight:'#fdecea',
    ok:'#1d7040', okLight:'#eaf5ee',
    shadow:'0 1px 3px rgba(0,0,0,0.07)', shadowHov:'0 3px 10px rgba(0,0,0,0.1)',
    divider:'#e0e6ea', rowHov:'#f7f9fb',
    navActive:'#eaf5ee', navActiveBorder:'#1d6e38',
    btnPrimary:'#1d6e38', btnPrimaryText:'#fff',
    tag:'#eaf5ee', tagText:'#1d6e38',
  },
};

const MOCK = {
  user: { name:'Carlos Mendoza', role:'Data Engineer', initials:'CM' },
  fundos: [
    { id:'f1', name:'El Palomar', region:'Lambayeque', ha:480, camas:5610, active:true, variedades:['Biloxi','Ventura','Emerald'],
      sectores:[
        { id:'s1a', name:'Sector A', modulos:8, camas:1240, variedad:'Biloxi', ha:120, turnos:2, valvulas:32 },
        { id:'s1b', name:'Sector B', modulos:12, camas:1850, variedad:'Ventura', ha:180, turnos:3, valvulas:48 },
        { id:'s1c', name:'Sector C', modulos:6, camas:980, variedad:'Emerald', ha:95, turnos:2, valvulas:24 },
        { id:'s1d', name:'Sector D', modulos:10, camas:1540, variedad:'Biloxi', ha:85, turnos:2, valvulas:40 },
      ]},
    { id:'f2', name:'La Victoria', region:'Piura', ha:320, camas:3890, active:true, variedades:['Springhigh',"O'Neal"],
      sectores:[
        { id:'s2a', name:'Sector Norte', modulos:9, camas:1100, variedad:'Springhigh', ha:160, turnos:2, valvulas:36 },
        { id:'s2b', name:'Sector Sur', modulos:7, camas:890, variedad:"O'Neal", ha:160, turnos:2, valvulas:28 },
      ]},
    { id:'f3', name:'Los Pinos', region:'La Libertad', ha:215, camas:1420, active:false, variedades:['Jewel'],
      sectores:[
        { id:'s3a', name:'Sector Único', modulos:5, camas:720, variedad:'Jewel', ha:215, turnos:1, valvulas:20 },
      ]},
  ],
  duplicates: [
    { id:1, tipo:'Variedad', campo:'Nombre', v1:'Biloxi', v2:'BILOXI', conf:98, estado:'pendiente', src:'tabla_variedades' },
    { id:2, tipo:'Variedad', campo:'Nombre', v1:"O'Neal", v2:'ONeal', conf:94, estado:'pendiente', src:'tabla_variedades' },
    { id:3, tipo:'Fundo', campo:'Nombre', v1:'El Palomar', v2:'Fundo El Palomar', conf:91, estado:'pendiente', src:'tabla_fundos' },
    { id:4, tipo:'Personal', campo:'RUC', v1:'20450398832', v2:'20450398832', conf:100, estado:'revisando', src:'tabla_personal' },
    { id:5, tipo:'Variedad', campo:'Código', v1:'BIL-001', v2:'BIL001', conf:88, estado:'pendiente', src:'tabla_variedades' },
    { id:6, tipo:'Fundo', campo:'Coord. GPS', v1:'-6.7891,-79.841', v2:'-6.7890,-79.841', conf:82, estado:'pendiente', src:'tabla_fundos' },
    { id:7, tipo:'Personal', campo:'DNI', v1:'42891034', v2:'42891034', conf:100, estado:'aprobado', src:'tabla_personal' },
  ],
  etlRuns: [
    { id:'ETL-2847', fecha:'20/04 14:32', dur:'4m 12s', regs:2847392, errs:3, warns:12, estado:'ok', tabla:'cosecha_diaria' },
    { id:'ETL-2846', fecha:'20/04 10:15', dur:'3m 58s', regs:2844201, errs:0, warns:8, estado:'ok', tabla:'cosecha_diaria' },
    { id:'ETL-2845', fecha:'20/04 06:02', dur:'5m 31s', regs:2840877, errs:7, warns:15, estado:'warning', tabla:'calibres' },
    { id:'ETL-2844', fecha:'19/04 22:00', dur:'4m 22s', regs:2836540, errs:0, warns:4, estado:'ok', tabla:'variedades' },
    { id:'ETL-2843', fecha:'19/04 18:00', dur:'6m 45s', regs:2831200, errs:22, warns:31, estado:'error', tabla:'geografia' },
    { id:'ETL-2842', fecha:'19/04 14:00', dur:'4m 01s', regs:2824680, errs:0, warns:6, estado:'ok', tabla:'cosecha_diaria' },
    { id:'ETL-2841', fecha:'19/04 10:00', dur:'3m 44s', regs:2820100, errs:1, warns:3, estado:'ok', tabla:'calibres' },
    { id:'ETL-2840', fecha:'19/04 06:00', dur:'4m 55s', regs:2815500, errs:0, warns:9, estado:'ok', tabla:'variedades' },
  ],
  personal: [
    { id:1, nombre:'Ana Torres', rol:'Data Scientist', estado:'activo', acceso:'Hace 10 min', modulos:['Modelos IA','Dashboard'], actividad:'Alta', acc:142 },
    { id:2, nombre:'Luis Paredes', rol:'Data Analyst', estado:'activo', acceso:'Hace 1h', modulos:['Homologación','ETL'], actividad:'Media', acc:58 },
    { id:3, nombre:'María Gonzales', rol:'Data Engineer', estado:'activo', acceso:'Hoy 09:20', modulos:['Pipeline','Geografía'], actividad:'Alta', acc:214 },
    { id:4, nombre:'Pedro Quispe', rol:'Gerente TI', estado:'activo', acceso:'Ayer 17:45', modulos:['Dashboard'], actividad:'Baja', acc:12 },
    { id:5, nombre:'Rosa Chávez', rol:'Data Analyst', estado:'inactivo', acceso:'15/04/2026', modulos:[], actividad:'—', acc:0 },
    { id:6, nombre:'Jorge Llanos', rol:'Data Scientist', estado:'pendiente', acceso:'—', modulos:[], actividad:'—', acc:0 },
  ],
  cosecha: {
    todos: [
      { mes:'Oct', real:320, proj:310 }, { mes:'Nov', real:580, proj:560 },
      { mes:'Dic', real:940, proj:920 }, { mes:'Ene', real:1240, proj:1200 },
      { mes:'Feb', real:980, proj:1050 }, { mes:'Mar', real:760, proj:780 },
      { mes:'Abr', real:null, proj:520 }, { mes:'May', real:null, proj:290 },
    ],
    f1: [
      { mes:'Oct', real:180, proj:175 }, { mes:'Nov', real:320, proj:310 },
      { mes:'Dic', real:530, proj:520 }, { mes:'Ene', real:700, proj:680 },
      { mes:'Feb', real:560, proj:590 }, { mes:'Mar', real:430, proj:440 },
      { mes:'Abr', real:null, proj:290 }, { mes:'May', real:null, proj:160 },
    ],
    f2: [
      { mes:'Oct', real:120, proj:115 }, { mes:'Nov', real:220, proj:210 },
      { mes:'Dic', real:360, proj:350 }, { mes:'Ene', real:490, proj:470 },
      { mes:'Feb', real:380, proj:410 }, { mes:'Mar', real:290, proj:300 },
      { mes:'Abr', real:null, proj:195 }, { mes:'May', real:null, proj:110 },
    ],
    f3: [
      { mes:'Oct', real:20, proj:20 }, { mes:'Nov', real:40, proj:40 },
      { mes:'Dic', real:50, proj:50 }, { mes:'Ene', real:50, proj:50 },
      { mes:'Feb', real:40, proj:50 }, { mes:'Mar', real:40, proj:40 },
      { mes:'Abr', real:null, proj:35 }, { mes:'May', real:null, proj:20 },
    ],
  },
  calibres: [
    { cal:'18mm+', pct:12 }, { cal:'16–18mm', pct:28 },
    { cal:'14–16mm', pct:35 }, { cal:'12–14mm', pct:18 }, { cal:'<12mm', pct:7 },
  ],
  health: [
    { key:'db', name:'Oracle DB', status:'ok', detail:'Latencia 12ms', check:'hace 30s' },
    { key:'etl', name:'ETL Runner', status:'ok', detail:'En ejecución', check:'hace 2m' },
    { key:'cosecha', name:'Modelo Cosecha', status:'ok', detail:'Última ejecución 6h', check:'hace 6h' },
    { key:'calibre', name:'Modelo Calibres', status:'warning', detail:'Actualización pendiente', check:'hace 18h' },
  ],
  reglas: [
    { id:'R001', nombre:'Cama sin variedad asignada', tabla:'t_cama', tipo:'Obligatorio', activa:true, violaciones:3 },
    { id:'R002', nombre:'Sector fuera de fundo activo', tabla:'t_sector', tipo:'Integridad', activa:true, violaciones:0 },
    { id:'R003', nombre:'Calibre fuera de rango (8–24mm)', tabla:'t_calibre', tipo:'Rango', activa:true, violaciones:12 },
    { id:'R004', nombre:'Cosecha sin turno asociado', tabla:'t_cosecha', tipo:'Obligatorio', activa:false, violaciones:0 },
    { id:'R005', nombre:'Válvula sin módulo padre', tabla:'t_valvula', tipo:'Integridad', activa:true, violaciones:1 },
  ],
};

const IPATHS = {
  home:["M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z","M9 22V12h6v10"],
  map:["M21 10c0 7-9 13-9 13S3 17 3 10a9 9 0 0 1 18 0z","M12 7a3 3 0 1 0 0 6 3 3 0 0 0 0-6z"],
  shuffle:["M16 3h5v5","M4 20L21 3","M21 16v5h-5","M15 15l6.1 6.1","M4 4l5 5"],
  git:["M6 3v12","M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6z","M6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6z","M18 9a9 9 0 0 1-9 9"],
  file:["M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z","M14 2v6h6","M16 13H8","M16 17H8","M10 9H8"],
  shield:["M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"],
  users:["M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2","M23 21v-2a4 4 0 0 0-3-3.87","M16 3.13a4 4 0 0 1 0 7.75","M9 3a4 4 0 1 0 0 8 4 4 0 0 0 0-8z"],
  cpu:["M18 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2z","M9 9h6v6H9z","M12 2v2","M12 20v2","M2 12h2","M20 12h2"],
  activity:["M22 12h-4l-3 9L9 3l-3 9H2"],
  settings:["M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z","M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"],
  bell:["M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9","M13.73 21a2 2 0 0 1-3.46 0"],
  search:["M11 17a6 6 0 1 0 0-12 6 6 0 0 0 0 12z","M21 21l-4.35-4.35"],
  chevR:["M9 18l6-6-6-6"], chevD:["M6 9l6 6 6-6"],
  checkC:["M22 11.08V12a10 10 0 1 1-5.93-9.14","M22 4L12 14.01l-3-3"],
  alertC:["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z","M12 8v4","M12 16h.01"],
  xC:["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z","M15 9l-6 6","M9 9l6 6"],
  trendUp:["M23 6l-9.5 9.5-5-5L1 18","M17 6h6v6"],
  db:["M12 2C6.48 2 2 4.69 2 8s4.48 6 10 6 10-2.69 10-6-4.48-6-10-6z","M2 8v4c0 3.31 4.48 6 10 6s10-2.69 10-6V8","M2 12v4c0 3.31 4.48 6 10 6s10-2.69 10-6v-4"],
  layers:["M12 2L2 7l10 5 10-5-10-5z","M2 17l10 5 10-5","M2 12l10 5 10-5"],
  filter:["M22 3H2l8 9.46V19l4 2v-8.54L22 3z"],
  plus:["M12 5v14","M5 12h14"],
  logout:["M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4","M16 17l5-5-5-5","M21 12H9"],
  eye:["M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z","M12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z"],
  bar:["M18 20V10","M12 20V4","M6 20v-6"],
  zap:["M13 2L3 14h9l-1 8 10-12h-9l1-8z"],
  check:["M20 6L9 17l-5-5"], x:["M18 6L6 18","M6 6l12 12"],
  edit:["M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7","M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"],
  pkg:["M16.5 9.4l-9-5.19","M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z","M3.27 6.96L12 12.01l8.73-5.05","M12 22.08V12"],
  info:["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z","M12 8h.01","M11 12h1v4h1"],
  refresh:["M23 4v6h-6","M1 20v-6h6","M3.51 9a9 9 0 0 1 14.85-3.36L23 10","M1 14l4.64 4.36A9 9 0 0 0 20.49 15"],
  download:["M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4","M7 10l5 5 5-5","M12 15V3"],
  award:["M12 15a7 7 0 1 0 0-14 7 7 0 0 0 0 14z","M8.21 13.89L7 23l5-3 5 3-1.21-9.12"],
  globe:["M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20z","M2 12h20","M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"],
  star:["M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"],
  xSquare:["M15 3H9a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z","M9 9l6 6","M15 9l-6 6"],
};

const Icon = ({ name, size=18, color, sw=1.8, style }) => {
  const t = useTheme();
  const stroke = color || (t ? t.textMuted : '#888');
  const paths = IPATHS[name] || [];
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24"
      fill="none" stroke={stroke} strokeWidth={sw} strokeLinecap="round" strokeLinejoin="round" style={style}>
      {paths.map((d,i) => <path key={i} d={d}/>)}
    </svg>
  );
};

Object.assign(window, { ThemeCtx, useTheme, FiltersCtx, FiltersProvider, useFilters, THEMES, MOCK, IPATHS, Icon });
