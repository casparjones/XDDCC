import { createRxDatabase, addRxPlugin } from 'https://esm.sh/rxdb@15';
import { getRxStorageDexie }             from 'https://esm.sh/rxdb@15/plugins/storage-dexie';
import { replicateRxCollection }         from 'https://esm.sh/rxdb@15/plugins/replication';
import { RxDBLeaderElectionPlugin }      from 'https://esm.sh/rxdb@15/plugins/leader-election';
import { Observable }                    from 'https://esm.sh/rxjs@7';

addRxPlugin(RxDBLeaderElectionPlugin);

const RXFT_TOKEN  = 'rxft_bab9f1c7b33f7a9376e0c65631a6af1e7e9382f41e476e4d2f7f0dbf077afd87';
const RXFORGE_APP = 'e4d1cafa-0c96-4645-8407-967a4eb14535';
const BASE_URL    = 'https://rxforge.de';
const SYNC_URL    = `${BASE_URL}/api/v1/sync/${RXFORGE_APP}`;
const MAX_SCORES  = 10;

const canvas         = document.getElementById('game');
const ctx            = canvas.getContext('2d');
const scoreEl        = document.getElementById('score');
const livesEl        = document.getElementById('lives');
const levelEl        = document.getElementById('level');
const restartBtn     = document.getElementById('restart');
const scoresEl       = document.getElementById('scores');
const clearScoresBtn = document.getElementById('clearScores');
const syncDot        = document.getElementById('syncDot');

function setSyncStatus(s) {
    syncDot.className = `sync-dot ${s}`;
    syncDot.title = { syncing:'Synchronisierung…', ok:'Synchronisiert', error:'Sync-Fehler' }[s] ?? '';
}

let _jwt = null;
async function getWriteToken() {
    const now = Date.now();
    if (_jwt && _jwt.exp - now > 120_000) return _jwt.token;
    const r = await fetch(`${BASE_URL}/api/v1/auth/token/exchange`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ token: RXFT_TOKEN }),
    });
    if (!r.ok) throw new Error(`Token exchange: ${r.status}`);
    const { access_token, expires_in } = await r.json();
    _jwt = { token: access_token, exp: now + expires_in*1000 };
    return _jwt.token;
}

const db = await createRxDatabase({ name:'arkanoid_scores_v2', storage: getRxStorageDexie() });
await db.addCollections({
    scores: {
        schema: {
            version:0, primaryKey:'id', type:'object',
            properties: {
                id:        { type:'string', maxLength:100 },
                name:      { type:'string', maxLength:16  },
                points:    { type:'number' },
                level:     { type:'number' },
                date:      { type:'string', maxLength:50  },
                updatedAt: { type:'number' },
            },
            required: ['id'],
        },
        conflictHandler: async ({ newDocumentState: n, realMasterState: m }) => {
            if (n.updatedAt === m.updatedAt) return { isEqual:true, documentData:m };
            return { isEqual:false, documentData: n.updatedAt >= m.updatedAt ? n : m };
        },
    },
});

function stripMeta(doc) {
    if (!doc) return null;
    const { _meta, _attachments, _rev, ...clean } = doc;
    return clean;
}

const pullStream$ = new Observable(sub => {
    let cancelled = false, retry = 1000;
    async function connect() {
        if (cancelled) return;
        try {
            const res = await fetch(`${SYNC_URL}/stream`, { headers:{ Authorization:`Bearer ${RXFT_TOKEN}` } });
            if (!res.ok) throw new Error(`SSE ${res.status}`);
            retry = 1000;
            const reader = res.body.getReader(), dec = new TextDecoder();
            let buf = '';
            while (!cancelled) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += dec.decode(value, { stream:true });
                const lines = buf.split('\n'); buf = lines.pop();
                for (const l of lines)
                    if (l.startsWith('data:')) try { sub.next(JSON.parse(l.slice(5).trim())); } catch(_){}
            }
        } catch(e) {
            if (!cancelled) { setSyncStatus('error'); setTimeout(connect, retry); retry = Math.min(retry*2, 30000); }
            return;
        }
        if (!cancelled) setTimeout(connect, 1000);
    }
    connect();
    return () => { cancelled = true; };
});

const replication = replicateRxCollection({
    collection: db.scores,
    replicationIdentifier: `rxforge-${RXFORGE_APP}`,
    live:true, retryTime:5000,
    pull: {
        batchSize:100, stream$:pullStream$,
        handler: async (ck, bs) => {
            try {
                const r = await fetch(`${SYNC_URL}/pull?checkpoint=${encodeURIComponent(ck??'')}&limit=${bs}`,
                    { headers:{ Authorization:`Bearer ${RXFT_TOKEN}` } });
                const d = await r.json();
                setSyncStatus('ok');
                return { documents: d.documents??[], checkpoint: d.checkpoint??ck };
            } catch(e) { setSyncStatus('error'); return { documents:[], checkpoint:ck }; }
        },
    },
    push: {
        batchSize:50,
        handler: async rows => {
            setSyncStatus('syncing');
            try {
                const jwt = await getWriteToken();
                const r = await fetch(`${SYNC_URL}/push`, {
                    method:'POST',
                    headers:{ Authorization:`Bearer ${jwt}`, 'Content-Type':'application/json' },
                    body: JSON.stringify({ rows: rows.map(row => ({
                        assumed_master_state: row.assumedMasterState ? stripMeta(row.assumedMasterState) : null,
                        new_document_state:   stripMeta(row.newDocumentState),
                    })) }),
                });
                const d = await r.json();
                setSyncStatus('ok');
                return d.conflicts ?? [];
            } catch(e) { setSyncStatus('error'); throw e; }
        },
    },
});
replication.error$.subscribe(e => { setSyncStatus('error'); console.error('[RxForge]', e); });

async function loadScores() {
    return (await db.scores.find().exec())
        .map(d => d.toJSON()).sort((a,b) => b.points-a.points).slice(0, MAX_SCORES);
}
async function qualifies(pts) {
    if (pts <= 0) return false;
    const list = await loadScores();
    return list.length < MAX_SCORES || pts > list[list.length-1].points;
}
let highlightId = null;
function genId() {
    const b = crypto.getRandomValues(new Uint8Array(16));
    b[6]=(b[6]&0x0f)|0x40; b[8]=(b[8]&0x3f)|0x80;
    const h = [...b].map(x=>x.toString(16).padStart(2,'0')).join('');
    return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}
async function submitScore(pts, name, lvl) {
    const e = { id:genId(), name:(name||'Anonym').slice(0,16), points:pts, level:lvl,
        date:new Date().toISOString(), updatedAt:Date.now() };
    highlightId = e.id;
    await db.scores.insert(e);
    setTimeout(()=>{ highlightId=null; }, 6000);
}
function renderScoresList(list) {
    scoresEl.innerHTML = '';
    if (!list.length) {
        const li = document.createElement('li');
        li.className='empty'; li.textContent='Noch keine Einträge!';
        li.style.gridTemplateColumns='1fr'; scoresEl.appendChild(li); return;
    }
    list.forEach(e => {
        const li=document.createElement('li');
        if (e.id===highlightId) li.className='new';
        const nm=document.createElement('span'); nm.textContent=e.name;
        const pts=document.createElement('span'); pts.className='points'; pts.textContent=e.points+' Pkt.';
        li.appendChild(nm); li.appendChild(pts); scoresEl.appendChild(li);
    });
}
db.scores.find().$.subscribe(docs =>
    renderScoresList(docs.map(d=>d.toJSON()).sort((a,b)=>b.points-a.points).slice(0,MAX_SCORES))
);
clearScoresBtn.addEventListener('click', async () => {
    if (confirm('Alle Highscores löschen?')) {
        const docs = await db.scores.find().exec();
        await Promise.all(docs.map(d=>d.remove()));
    }
});

// ═══════════════════════════════════════════════════════════════════════
// GAME
// ═══════════════════════════════════════════════════════════════════════
const W = canvas.width;   // 784
const H = canvas.height;  // 448

// ── 3×5 pixel font for text levels ────────────────────────────────────
// Each letter is a 5-row × 3-col binary grid (1=brick, 0=gap).
// Letters are rendered as actual bricks, not canvas text.
const FONT3 = {
    A: [[0,1,0],[1,0,1],[1,1,1],[1,0,1],[1,0,1]],
    B: [[1,1,0],[1,0,1],[1,1,0],[1,0,1],[1,1,0]],
    C: [[1,1,0],[1,0,0],[1,0,0],[1,0,0],[1,1,0]],
    D: [[1,1,0],[1,0,1],[1,0,1],[1,0,1],[1,1,0]],
    E: [[1,1,1],[1,0,0],[1,1,0],[1,0,0],[1,1,1]],
    G: [[0,1,1],[1,0,0],[1,0,1],[1,0,1],[0,1,1]],
    H: [[1,0,1],[1,0,1],[1,1,1],[1,0,1],[1,0,1]],
    I: [[1,1,1],[0,1,0],[0,1,0],[0,1,0],[1,1,1]],
    J: [[0,1,1],[0,0,1],[0,0,1],[1,0,1],[0,1,0]],
    K: [[1,0,1],[1,1,0],[1,0,0],[1,1,0],[1,0,1]],
    L: [[1,0,0],[1,0,0],[1,0,0],[1,0,0],[1,1,1]],
    M: [[1,0,1],[1,1,1],[1,0,1],[1,0,1],[1,0,1]],
    N: [[1,0,1],[1,1,0],[1,0,1],[1,0,1],[1,0,1]],
    O: [[0,1,0],[1,0,1],[1,0,1],[1,0,1],[0,1,0]],
    P: [[1,1,0],[1,0,1],[1,1,0],[1,0,0],[1,0,0]],
    R: [[1,1,0],[1,0,1],[1,1,0],[1,0,1],[1,0,1]],
    T: [[1,1,1],[0,1,0],[0,1,0],[0,1,0],[0,1,0]],
    U: [[1,0,1],[1,0,1],[1,0,1],[1,0,1],[0,1,0]],
    X: [[1,0,1],[1,0,1],[0,1,0],[1,0,1],[1,0,1]],
    Z: [[1,1,1],[0,0,1],[0,1,0],[1,0,0],[1,1,1]],
};
// Text-level brick dimensions (smaller, more columns to fit 6-letter words)
const BW_T=30, BH_T=16, BPADX_T=3, BPADY_T=8;
const CHAR_W=3, CHAR_H=5, CHAR_GAP=1;

// ── Constants ─────────────────────────────────────────────────────────
// Speed is in logical px per "normalized frame" (1.0 = 60 fps tick).
// Delta-time keeps this consistent across all frame rates and devices.
const BASE_SPEED   = 4.5;
const SPEED_INC    = 0.05;   // +5% per level
const MAX_BALL_SPD = 12;
const MAX_BALLS    = 3;
const BULLET_SPD   = 9;
const GUN_CD       = 22;     // normalized frames between bursts
const EFFECT_TICKS = 600;    // 10 s @ 60 fps
const PILL_SPD     = 1.8;
const PILL_W       = 42;
const PILL_H       = 14;
// Brick grid
const BW=50, BH=18, BPADX=6, BPADY=8, BTOP=50, BCOLS=14;

const ROW_COLORS = [
    '#ff4d4d','#ff8c00','#ffd700','#44dd44',
    '#22ccff','#9944ff','#ff44cc','#c8c8c8',
    '#ff6666','#ffaa33','#bbff33','#33ffcc',
];
const PILL_DEFS = [
    { type:'big_paddle',   color:'#22c55e', label:'+PAD', pos:true,  w:10 },
    { type:'small_paddle', color:'#ef4444', label:'-PAD', pos:false, w: 8 },
    { type:'gun',          color:'#3b82f6', label:'GUN',  pos:true,  w: 7 },
    { type:'extra_life',   color:'#f97316', label:'+♥',   pos:true,  w: 5 },
    { type:'slow_ball',    color:'#a855f7', label:'SLW',  pos:true,  w:10 },
    { type:'fast_ball',    color:'#ec4899', label:'FST',  pos:false, w: 8 },
    { type:'extra_ball',   color:'#dc2626', label:'+BLL', pos:false, w: 6 },
    { type:'diamond',      color:'#00e5ff', label:'💎',   pos:true,  w: 3 },
];

const LEVELS = [
    { rows:3, pattern:'full',    paddleW:110, toughRows:0 },               //  1
    { rows:5, pattern:'text',    paddleW:100, toughRows:0, text:'POMNI'  },//  2
    { rows:5, pattern:'pyramid', paddleW:100, toughRows:0 },               //  3
    { rows:5, pattern:'text',    paddleW: 95, toughRows:0, text:'KATHA'  },//  4
    { rows:6, pattern:'checker', paddleW: 95, toughRows:1 },               //  5
    { rows:5, pattern:'text',    paddleW: 90, toughRows:0, text:'RAGA'   },//  6
    { rows:7, pattern:'gaps',    paddleW: 90, toughRows:1 },               //  7
    { rows:5, pattern:'text',    paddleW: 86, toughRows:0, text:'JAX'    },//  8
    { rows:7, pattern:'diamond', paddleW: 86, toughRows:2 },               //  9
    { rows:6, pattern:'text',    paddleW: 83, toughRows:1, text:'KINGER' },// 10
    { rows:7, pattern:'zigzag',  paddleW: 83, toughRows:2 },               // 11
    { rows:5, pattern:'text',    paddleW: 80, toughRows:1, text:'XDCC'   },// 12
    { rows:8, pattern:'wave',    paddleW: 80, toughRows:2 },               // 13
    { rows:6, pattern:'text',    paddleW: 77, toughRows:1, text:'GANGLE' },// 14
    { rows:8, pattern:'fortress',paddleW: 77, toughRows:3 },               // 15
    { rows:6, pattern:'text',    paddleW: 74, toughRows:1, text:'ZOOBLE' },// 16
    { rows:9, pattern:'full',    paddleW: 74, toughRows:3 },               // 17
    { rows:6, pattern:'text',    paddleW: 70, toughRows:2, text:'CAINE'  },// 18
    { rows:9, pattern:'cross',   paddleW: 70, toughRows:3 },               // 19
    { rows:7, pattern:'text',    paddleW: 65, toughRows:2, text:'BUBBLE' },// 20
];

// ── State ─────────────────────────────────────────────────────────────
const paddle = { w:110, h:12, x:W/2-55, y:H-30, speed:7 };
let balls=[], bricks=[], particles=[], pills=[], bullets=[];
let effects={}, gunTimer=0;
let score=0, lives=3, level=1;
let gameOver=false, won=false, scoreHandled=false;

function resetEffects() {
    effects = { bigPaddle:0, smallPaddle:0, gun:0, slowBall:0, fastBall:0 };
}
function levelCfg() { return LEVELS[Math.min(level, LEVELS.length)-1]; }
function ballSpeed() { return BASE_SPEED * (1 + (level-1) * SPEED_INC); }

// ── Pattern visibility ────────────────────────────────────────────────
function patternVisible(p, r, c, rows) {
    switch(p) {
        case 'full':    return true;
        case 'pyramid': { const i=rows-1-r; return c>=i && c<BCOLS-i; }
        case 'gaps':    return c%3!==2;
        case 'checker': return (r+c)%2===0 || r===0 || r===rows-1;
        case 'diamond': {
            const cr=(rows-1)/2, cc=(BCOLS-1)/2;
            return Math.abs(r-cr)/(rows/2)+Math.abs(c-cc)/(BCOLS/2) <= 1.05;
        }
        case 'zigzag':  return (c+r*3)%9<7;
        case 'wave': {
            const top = Math.round(Math.sin(c/BCOLS*Math.PI*2.5)*2+2.5);
            return r>=top;
        }
        case 'fortress':
            return r===0||r===rows-1||c===0||c===BCOLS-1||
                   (r>=2&&r<=rows-3&&c>=3&&c<=BCOLS-4&&(r+c)%2===0);
        case 'cross': {
            const mc=Math.floor(BCOLS/2), mr=Math.floor(rows/2);
            return r===mr||(c>=mc-1&&c<=mc+1)||r===0||r===rows-1;
        }
        default: return true;
    }
}

// ── Build bricks from FONT3 pixel font ───────────────────────────────
function makeTextBricks(text, hits) {
    const result = [];
    const totalFontCols = text.length * (CHAR_W + CHAR_GAP) - CHAR_GAP;
    const totalPixW = totalFontCols * BW_T + (totalFontCols - 1) * BPADX_T;
    const offL = (W - totalPixW) / 2;

    for (let ci = 0; ci < text.length; ci++) {
        const glyph = FONT3[text[ci]];
        if (!glyph) continue;
        const colBase = ci * (CHAR_W + CHAR_GAP);
        for (let gr = 0; gr < CHAR_H; gr++) {
            for (let gc = 0; gc < CHAR_W; gc++) {
                if (!glyph[gr][gc]) continue;
                const col = colBase + gc;
                const row = gr;
                result.push({
                    x: offL + col * (BW_T + BPADX_T),
                    y: BTOP  + row * (BH_T + BPADY_T),
                    bw: BW_T, bh: BH_T,
                    maxHits: hits, hits, alive: true, isDyna: false,
                    row, col,
                    color: ROW_COLORS[row % ROW_COLORS.length],
                });
            }
        }
    }
    return result;
}

// ── Build bricks ──────────────────────────────────────────────────────
function buildBricks() {
    bricks=[];
    const cfg=levelCfg();

    if (cfg.pattern==='text') {
        const hits=cfg.toughRows>0?2:1;
        bricks = makeTextBricks(cfg.text, hits);
        if (!bricks.length) { // fallback
            const offL=(W-(BCOLS*BW+(BCOLS-1)*BPADX))/2;
            for (let c=0;c<BCOLS;c++)
                bricks.push({x:offL+c*(BW+BPADX),y:BTOP,bw:BW,bh:BH,
                    maxHits:1,hits:1,alive:true,isDyna:false,row:0,col:c,color:ROW_COLORS[0]});
        }
    } else {
        const totalW=BCOLS*BW+(BCOLS-1)*BPADX;
        const offL=(W-totalW)/2;
        for (let r=0;r<cfg.rows;r++) for (let c=0;c<BCOLS;c++) {
            if (!patternVisible(cfg.pattern,r,c,cfg.rows)) continue;
            const hits=Math.min(3,Math.max(1,cfg.toughRows-r+1));
            bricks.push({ x:offL+c*(BW+BPADX), y:BTOP+r*(BH+BPADY),
                bw:BW, bh:BH,
                maxHits:hits, hits, alive:true, isDyna:Math.random()<0.08,
                row:r, col:c, color:ROW_COLORS[r%ROW_COLORS.length] });
        }
    }
}

// ── Particles ─────────────────────────────────────────────────────────
function explode(x,y,color,n=10) {
    for (let i=0;i<n;i++) {
        const a=Math.random()*Math.PI*2, s=1.4+Math.random()*3;
        particles.push({ x,y, vx:Math.cos(a)*s, vy:Math.sin(a)*s-0.8,
            r:1.5+Math.random()*2.5, life:1, decay:0.03+Math.random()*0.04, color });
    }
}

// ── Kill brick ────────────────────────────────────────────────────────
function killBrick(b, isChain=false) {
    if (!b.alive) return;
    b.alive=false;
    explode(b.x+BW/2, b.y+BH/2, b.isDyna?'#ff6600':b.color, b.isDyna?18:12);
    if (!isChain) maybeDrop(b);
    if (isChain) { score+=5*b.maxHits; updateHud(); }
    if (b.isDyna) {
        const br=b.row, bc=b.col;
        setTimeout(()=>{
            if (gameOver) return;
            for (const nb of bricks)
                if (nb.alive && Math.abs(nb.row-br)<=1 && Math.abs(nb.col-bc)<=1)
                    killBrick(nb, true);
        }, 160);
    }
}

// ── Pill drop ─────────────────────────────────────────────────────────
function maybeDrop(b) {
    if (Math.random()>0.22) return;
    let pool=PILL_DEFS.map(d=>({...d}));
    if (balls.length>=MAX_BALLS) pool=pool.filter(d=>d.type!=='extra_ball');
    const life=pool.find(d=>d.type==='extra_life');
    if (life) { if (lives<=1) life.w=25; else if (lives<=2) life.w=14; }
    const total=pool.reduce((s,d)=>s+d.w,0);
    let rand=Math.random()*total, chosen=pool[pool.length-1];
    for (const d of pool) { rand-=d.w; if (rand<=0){chosen=d;break;} }
    pills.push({ x:b.x+BW/2-PILL_W/2, y:b.y+BH/2, vy:PILL_SPD, ...chosen });
}

// ── Speed helpers ─────────────────────────────────────────────────────
function normSpeeds(tgt) {
    for (const b of balls) {
        if (b.stuck) continue;
        const s=Math.hypot(b.vx,b.vy);
        if (s>0) { b.vx=b.vx/s*tgt; b.vy=b.vy/s*tgt; }
    }
}
function effectSpeed() {
    return effects.slowBall>0 ? ballSpeed()*0.62
         : effects.fastBall>0 ? ballSpeed()*1.5 : ballSpeed();
}

// ── Apply pill ────────────────────────────────────────────────────────
function applyPill(type) {
    const cfg=levelCfg();
    switch(type) {
        case 'big_paddle':
            effects.bigPaddle=EFFECT_TICKS; effects.smallPaddle=0;
            paddle.w=Math.min(W*0.5, cfg.paddleW*1.6); break;
        case 'small_paddle':
            effects.smallPaddle=EFFECT_TICKS; effects.bigPaddle=0;
            paddle.w=Math.max(38, cfg.paddleW*0.55); break;
        case 'gun':
            effects.gun=EFFECT_TICKS; gunTimer=0; break;
        case 'extra_life':
            lives=Math.min(9,lives+1); updateHud(); break;
        case 'slow_ball':
            if (effects.fastBall>0){effects.fastBall=0;normSpeeds(ballSpeed());}
            else {effects.slowBall=EFFECT_TICKS;normSpeeds(ballSpeed()*0.62);} break;
        case 'fast_ball':
            if (effects.slowBall>0){effects.slowBall=0;normSpeeds(ballSpeed());}
            else {effects.fastBall=EFFECT_TICKS;normSpeeds(ballSpeed()*1.5);} break;
        case 'extra_ball':
            if (balls.length<MAX_BALLS) {
                const spd=effectSpeed();
                balls.push({ x:paddle.x+paddle.w/2+(Math.random()-0.5)*16,
                    y:paddle.y-12, r:6, stuck:false,
                    vx:(Math.random()-0.5)*2.5, vy:-spd });
            }
            break;
        case 'diamond':
            score+=500+level*100; updateHud();
            explode(paddle.x+paddle.w/2, paddle.y, '#00e5ff', 20); break;
    }
}

// ── Ball helpers ──────────────────────────────────────────────────────
function newStuckBall() {
    return { x:paddle.x+paddle.w/2, y:paddle.y-8, r:6, stuck:true, vx:0, vy:0 };
}
function launchBall() {
    const b=balls.find(b=>b.stuck);
    if (!b) return;
    b.stuck=false;
    const a=(Math.random()*0.6-0.3)-Math.PI/2;
    const s=ballSpeed();
    b.vx=Math.cos(a)*s; b.vy=Math.sin(a)*s;
}
function applyLevelSetup() {
    const cfg=levelCfg();
    paddle.w=cfg.paddleW; paddle.x=W/2-paddle.w/2;
    balls=[newStuckBall()]; bullets=[]; pills=[]; particles=[];
    resetEffects(); buildBricks();
}
function resetGame() {
    score=0; lives=3; level=1;
    gameOver=false; won=false; scoreHandled=false;
    applyLevelSetup(); updateHud();
}
function nextLevel() { level++; applyLevelSetup(); updateHud(); }
function updateHud() {
    scoreEl.textContent=score; livesEl.textContent=lives; levelEl.textContent=level;
}
async function endGame(didWin) {
    if (gameOver) return;
    gameOver=true; won=didWin;
    if (scoreHandled) return;
    scoreHandled=true;
    await new Promise(r=>setTimeout(r,50));
    if (!(await qualifies(score))) return;
    const def=localStorage.getItem('xddcc.arkanoid.lastName')||'';
    const name=(prompt('🎉 Highscore! Dein Name:',def)||'').trim();
    try { localStorage.setItem('xddcc.arkanoid.lastName',name); } catch(_){}
    await submitScore(score, name, level);
}

// ── Input ─────────────────────────────────────────────────────────────
const keys={};
document.addEventListener('keydown', e=>{
    keys[e.key]=true;
    if (e.key===' '&&!gameOver&&balls.some(b=>b.stuck)){launchBall();e.preventDefault();}
});
document.addEventListener('keyup', e=>{ keys[e.key]=false; });

canvas.addEventListener('mousemove', e=>{
    const r=canvas.getBoundingClientRect();
    paddle.x=Math.max(0,Math.min(W-paddle.w,(e.clientX-r.left)*(W/r.width)-paddle.w/2));
});
canvas.addEventListener('click', ()=>{ if(balls.some(b=>b.stuck)&&!gameOver) launchBall(); });
canvas.addEventListener('touchmove', e=>{
    e.preventDefault();
    const r=canvas.getBoundingClientRect();
    paddle.x=Math.max(0,Math.min(W-paddle.w,(e.touches[0].clientX-r.left)*(W/r.width)-paddle.w/2));
},{passive:false});
canvas.addEventListener('touchstart', e=>{
    e.preventDefault();
    const r=canvas.getBoundingClientRect();
    paddle.x=Math.max(0,Math.min(W-paddle.w,(e.touches[0].clientX-r.left)*(W/r.width)-paddle.w/2));
    if (balls.some(b=>b.stuck)&&!gameOver) launchBall();
},{passive:false});
restartBtn.addEventListener('click', resetGame);

// ── Update (dt = normalized to 60 fps, so dt≈1 at 60fps, dt≈2 at 30fps) ──
function update(dt) {
    if (gameOver) return;

    if (keys['ArrowLeft'] ||keys['a']||keys['A']) paddle.x-=paddle.speed*dt;
    if (keys['ArrowRight']||keys['d']||keys['D']) paddle.x+=paddle.speed*dt;
    paddle.x=Math.max(0,Math.min(W-paddle.w,paddle.x));

    // Effect timers (float countdown, consistent across frame rates)
    if (effects.bigPaddle>0)   { effects.bigPaddle  -=dt; if(effects.bigPaddle  <=0){effects.bigPaddle  =0;paddle.w=levelCfg().paddleW;} }
    if (effects.smallPaddle>0) { effects.smallPaddle-=dt; if(effects.smallPaddle<=0){effects.smallPaddle=0;paddle.w=levelCfg().paddleW;} }
    if (effects.gun>0)      effects.gun     -=dt;
    if (effects.slowBall>0) effects.slowBall-=dt;
    if (effects.fastBall>0) effects.fastBall-=dt;

    // Gun auto-fire
    if (effects.gun>0) {
        gunTimer-=dt;
        if (gunTimer<=0) {
            bullets.push({x:paddle.x+6,         y:paddle.y-3});
            bullets.push({x:paddle.x+paddle.w-6, y:paddle.y-3});
            gunTimer=GUN_CD;
        }
    }

    // Bullets
    for (let i=bullets.length-1;i>=0;i--) {
        const bu=bullets[i]; bu.y-=BULLET_SPD*dt;
        if (bu.y<0){bullets.splice(i,1);continue;}
        for (const b of bricks) {
            if (!b.alive) continue;
            if (bu.x>b.x&&bu.x<b.x+BW&&bu.y>b.y&&bu.y<b.y+BH) {
                b.hits--;
                if (b.hits<=0){score+=10*b.maxHits;killBrick(b);}
                else{score+=5;explode(bu.x,bu.y,b.color,4);}
                updateHud(); bullets.splice(i,1); break;
            }
        }
    }

    // Stuck ball follows paddle
    const stuck=balls.find(b=>b.stuck);
    if (stuck){stuck.x=paddle.x+paddle.w/2;stuck.y=paddle.y-stuck.r-1;}

    // Moving balls
    for (let i=balls.length-1;i>=0;i--) {
        const b=balls[i];
        if (b.stuck) continue;
        b.x+=b.vx*dt; b.y+=b.vy*dt;

        if (b.x-b.r<0){b.x=b.r;   b.vx= Math.abs(b.vx);}
        if (b.x+b.r>W){b.x=W-b.r; b.vx=-Math.abs(b.vx);}
        if (b.y-b.r<0){b.y=b.r;   b.vy= Math.abs(b.vy);}

        // Paddle bounce
        if (b.vy>0 && b.y+b.r>=paddle.y && b.y+b.r<=paddle.y+paddle.h &&
            b.x>=paddle.x && b.x<=paddle.x+paddle.w) {
            const hit=(b.x-(paddle.x+paddle.w/2))/(paddle.w/2);
            const ang=hit*(Math.PI/3);
            let spd=Math.min(MAX_BALL_SPD,Math.hypot(b.vx,b.vy));
            const tgt=effectSpeed();
            spd=spd*0.88+tgt*0.12;
            b.vx=spd*Math.sin(ang);
            b.vy=-Math.abs(spd*Math.cos(ang));
            b.y=paddle.y-b.r-1;
        }

        // Brick collision
        for (const br of bricks) {
            if (!br.alive) continue;
            if (b.x+b.r>br.x&&b.x-b.r<br.x+BW&&b.y+b.r>br.y&&b.y-b.r<br.y+BH) {
                br.hits--; score+=10*br.maxHits;
                if (br.hits<=0) killBrick(br); else explode(b.x,b.y,br.color,5);
                updateHud();
                const side=(b.x-b.vx*dt)<br.x||(b.x-b.vx*dt)>br.x+BW;
                if (side) b.vx*=-1; else b.vy*=-1;
                break;
            }
        }
        if (b.y-b.r>H) balls.splice(i,1);
    }

    // All balls lost
    if (balls.length===0) {
        lives--; updateHud();
        if (lives<=0) endGame(false).catch(console.warn);
        else { pills=[]; bullets=[]; resetEffects(); paddle.w=levelCfg().paddleW; balls=[newStuckBall()]; }
    }

    // Pills
    for (let i=pills.length-1;i>=0;i--) {
        const p=pills[i]; p.y+=p.vy*dt;
        if (p.y+PILL_H>=paddle.y&&p.y<=paddle.y+paddle.h&&
            p.x+PILL_W>=paddle.x&&p.x<=paddle.x+paddle.w) {
            applyPill(p.type); pills.splice(i,1); continue;
        }
        if (p.y>H) pills.splice(i,1);
    }

    // Particles
    for (let i=particles.length-1;i>=0;i--) {
        const p=particles[i];
        p.x+=p.vx*dt; p.y+=p.vy*dt; p.vy+=0.09*dt;
        p.life-=p.decay*dt;
        if (p.life<=0) particles.splice(i,1);
    }

    if (!gameOver && bricks.every(b=>!b.alive)) {
        if (level>=LEVELS.length) endGame(true).catch(console.warn);
        else nextLevel();
    }
}

// ── Draw ──────────────────────────────────────────────────────────────
function draw() {
    ctx.clearRect(0,0,W,H);

    // Faint level name watermark
    const cfg=levelCfg();
    if (cfg.text) {
        ctx.save();
        ctx.globalAlpha=0.04;
        ctx.fillStyle='#fff';
        ctx.font=`bold ${Math.floor(H*0.52)}px monospace`;
        ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.fillText(cfg.text,W/2,H/2);
        ctx.restore();
    }

    // Bricks
    for (const b of bricks) {
        if (!b.alive) continue;
        if (b.isDyna) {
            ctx.fillStyle='#1a1030';
            ctx.fillRect(b.x,b.y,BW,BH);
            ctx.strokeStyle='#ff6600'; ctx.lineWidth=2;
            ctx.strokeRect(b.x+1,b.y+1,BW-2,BH-2);
            ctx.font='11px sans-serif';
            ctx.textAlign='center'; ctx.textBaseline='middle';
            ctx.fillStyle='#ff6600';
            ctx.fillText('💣',b.x+BW/2,b.y+BH/2);
        } else {
            const hp=b.hits/b.maxHits;
            ctx.globalAlpha=0.45+hp*0.55;
            ctx.fillStyle=b.color; ctx.fillRect(b.x,b.y,BW,BH);
            ctx.fillStyle='rgba(255,255,255,0.18)'; ctx.fillRect(b.x,b.y,BW,BH/2);
            ctx.globalAlpha=1;
            ctx.strokeStyle='rgba(255,255,255,0.22)'; ctx.lineWidth=1;
            ctx.strokeRect(b.x,b.y,BW,BH);
            if (b.maxHits>1) {
                ctx.fillStyle='rgba(255,255,255,0.9)';
                ctx.font='bold 10px sans-serif';
                ctx.textAlign='center'; ctx.textBaseline='middle';
                ctx.fillText(b.hits,b.x+BW/2,b.y+BH/2);
            }
        }
    }

    // Particles
    ctx.save();
    for (const p of particles) {
        ctx.globalAlpha=Math.max(0,p.life);
        ctx.fillStyle=p.color;
        ctx.beginPath(); ctx.arc(p.x,p.y,p.r,0,Math.PI*2); ctx.fill();
    }
    ctx.restore();

    // Pills
    ctx.save();
    for (const p of pills) {
        ctx.globalAlpha=0.93; ctx.fillStyle=p.color;
        ctx.beginPath(); ctx.roundRect(p.x,p.y,PILL_W,PILL_H,PILL_H/2); ctx.fill();
        ctx.strokeStyle=p.pos?'rgba(255,255,255,0.85)':'rgba(255,160,160,0.85)';
        ctx.lineWidth=1.5; ctx.globalAlpha=1; ctx.stroke();
        ctx.fillStyle='#fff';
        ctx.font=p.label==='💎'?'10px sans-serif':'bold 8px sans-serif';
        ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.fillText(p.label,p.x+PILL_W/2,p.y+PILL_H/2);
    }
    ctx.restore();

    // Bullets
    ctx.fillStyle='#fffb00';
    for (const bu of bullets) {
        ctx.fillRect(bu.x-2,bu.y,4,9);
        ctx.fillStyle='#fff'; ctx.fillRect(bu.x-1,bu.y,2,3); ctx.fillStyle='#fffb00';
    }

    // Paddle
    const pc = effects.gun>0       ? '#3b82f6'
             : effects.bigPaddle>0   ? '#22c55e'
             : effects.smallPaddle>0 ? '#ef4444' : '#ffffff';
    const pg=ctx.createLinearGradient(0,paddle.y,0,paddle.y+paddle.h);
    pg.addColorStop(0,pc); pg.addColorStop(1,shadeColor(pc,-40));
    ctx.fillStyle=pg; ctx.fillRect(paddle.x,paddle.y,paddle.w,paddle.h);
    ctx.strokeStyle='rgba(255,255,255,0.4)'; ctx.lineWidth=1;
    ctx.strokeRect(paddle.x,paddle.y,paddle.w,paddle.h);
    if (effects.gun>0) {
        ctx.fillStyle='#93c5fd';
        ctx.fillRect(paddle.x+3,       paddle.y-7,4,9);
        ctx.fillRect(paddle.x+paddle.w-7,paddle.y-7,4,9);
    }

    // Balls
    for (const b of balls) {
        const g=ctx.createRadialGradient(b.x-1.5,b.y-1.5,1,b.x,b.y,b.r);
        g.addColorStop(0,'#ffe566'); g.addColorStop(1,'#ff8c00');
        ctx.fillStyle=g;
        ctx.beginPath(); ctx.arc(b.x,b.y,b.r,0,Math.PI*2); ctx.fill();
        ctx.strokeStyle='rgba(255,255,255,0.5)'; ctx.lineWidth=0.8; ctx.stroke();
    }

    drawEffectsBar();

    // Overlay
    if (gameOver) {
        ctx.fillStyle='rgba(0,0,0,0.65)';
        ctx.fillRect(0,H/2-50,W,100);
        ctx.fillStyle='#fff'; ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.font=`bold 34px sans-serif`;
        ctx.fillText(won?'Gewonnen! 🎉':'Game Over',W/2,H/2-10);
        ctx.font='18px sans-serif';
        ctx.fillText('Punkte: '+score,W/2,H/2+24);
    } else if (balls.some(b=>b.stuck)) {
        ctx.fillStyle='rgba(255,255,255,0.85)';
        ctx.textAlign='center'; ctx.textBaseline='middle';
        ctx.font='17px sans-serif';
        ctx.fillText('Tippen / Klick / Leertaste zum Starten',W/2,H/2);
        ctx.font='13px sans-serif'; ctx.fillStyle='rgba(255,183,3,0.85)';
        ctx.fillText(`Level ${level} / ${LEVELS.length}`,W/2,H/2+24);
    }
}

function drawEffectsBar() {
    const act=[];
    if (effects.bigPaddle>0)   act.push({label:'+PAD',color:'#22c55e',t:effects.bigPaddle});
    if (effects.smallPaddle>0) act.push({label:'-PAD',color:'#ef4444',t:effects.smallPaddle});
    if (effects.gun>0)         act.push({label:'GUN', color:'#3b82f6',t:effects.gun});
    if (effects.slowBall>0)    act.push({label:'SLW', color:'#a855f7',t:effects.slowBall});
    if (effects.fastBall>0)    act.push({label:'FST', color:'#ec4899',t:effects.fastBall});
    if (!act.length) return;
    const iw=40,ih=13,gap=4,y0=H-17;
    ctx.font='bold 8px sans-serif'; ctx.textAlign='center'; ctx.textBaseline='middle';
    for (let i=0;i<act.length;i++) {
        const e=act[i], x=6+i*(iw+gap), ratio=Math.max(0,e.t/EFFECT_TICKS);
        ctx.fillStyle=e.color; ctx.globalAlpha=0.2+ratio*0.75;
        ctx.beginPath(); ctx.roundRect(x,y0,iw,ih,3); ctx.fill();
        ctx.strokeStyle=e.color; ctx.lineWidth=1; ctx.globalAlpha=0.6+ratio*0.4; ctx.stroke();
        ctx.globalAlpha=1; ctx.fillStyle='#fff';
        ctx.fillText(e.label,x+iw/2,y0+ih/2);
    }
    ctx.globalAlpha=1;
}

function shadeColor(hex,amt) {
    const n=parseInt(hex.replace('#',''),16);
    return '#'+[n>>16,(n>>8)&0xff,n&0xff]
        .map(v=>Math.max(0,Math.min(255,v+amt)).toString(16).padStart(2,'0')).join('');
}

// ── Delta-time game loop ──────────────────────────────────────────────
// dt is normalized to 60 fps: dt=1.0 at 60fps, dt=0.5 at 120fps, dt=2.0 at 30fps.
// This keeps ball speed identical in wall-clock time regardless of device frame rate.
let lastTs = null;
function loop(ts) {
    if (lastTs === null) { lastTs = ts; requestAnimationFrame(loop); return; }
    const dt = Math.min((ts - lastTs) / 16.667, 2.5); // cap at 2.5× to handle tab-focus lag
    lastTs = ts;
    update(dt);
    draw();
    requestAnimationFrame(loop);
}

resetGame();
requestAnimationFrame(loop);
