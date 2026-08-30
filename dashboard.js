// ════════════════════════════════════════════════════════
// 設定: シート名マッピング
// ════════════════════════════════════════════════════════
const INDICATORS = [
  {
    category: '貴金属・エネルギー',
    items: [
      { key: '金価格',      label: '金価格',      unit: 'USD / oz',   color: '#e8c84a', sheetName: '金価格',      col: 1,
        note: 'LBMA（ロンドン貴金属市場）午後金価格 USD/トロイオンス 月次＋直近週次' },
      { key: '銅価格',      label: '銅価格',      unit: 'USD / MT',   color: '#e8714a', sheetName: '銅価格',      col: 1,
        note: 'LME（ロンドン金属取引所）銅公式清算価格 USD/MT（メトリックトン＝1,000kg） 月次＋直近週次' },
      { key: '原油WTI',     label: 'WTI原油',     unit: 'USD / bbl',  color: '#4a9ee8', sheetName: '原油価格WTI', col: 1,
        note: 'WTI（ウエスト・テキサス・インターミディエート）先物 USD/バレル 月次＋直近週次' },
    ]
  },
  {
    category: '資源・素材',
    items: [
      { key: '石炭価格',    label: '石炭価格',    unit: 'USD / ton',  color: '#9da5bf', sheetName: '石炭価格',    col: 1,
        note: 'オーストラリア産一般炭（Newcastle）6,300 kcal/kg GAR 月次平均 ※インドネシア炭4,200 kcalとは品質・価格帯が異なります' },
      { key: '鉄鉱石価格',  label: '鉄鉱石価格',  unit: 'USD / dmtu', color: '#c47ae8', sheetName: '鉄鉱石価格',  col: 1,
        note: '中国向けスポット価格（CFR青島）62% Fe 月次平均' },
      { key: 'ニッケル価格', label: 'ニッケル価格', unit: 'USD / mt',  color: '#a8e84a', sheetName: 'ニッケル価格', col: 1,
        note: 'LME（ロンドン金属取引所）ニッケル公式清算価格 USD/MT（メトリックトン＝1,000kg） 月次平均' },
    ]
  },
  {
    category: '建設・住宅',
    items: [
      { key: '北米住宅着工', label: '北米住宅着工', unit: '千件',  color: '#4ae8a0', sheetName: '北米住宅着工',   col: 1 },
      { key: 'EU建設指数',   label: 'EU住宅着工',    unit: '件 / 月',  color: '#4ae8e8', sheetName: '欧州建設生産指数', col: 1 },
    ]
  }
];

// ════════════════════════════════════════════════════════
// data.json からデータ取得（Google Sheets API の代替）
// ════════════════════════════════════════════════════════
let _jsonData = null;

async function loadDataJSON() {
  if (_jsonData) return _jsonData;
  const resp = await fetch('./data.json');
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  _jsonData = await resp.json();
  return _jsonData;
}

function getSheetData(jsonData, sheetName) {
  const rows = (jsonData.sheets || {})[sheetName];
  if (!rows || rows.length === 0) throw new Error('No data');
  return rows.map(([date, value]) => ({ date, value: parseFloat(value) }))
             .filter(d => !isNaN(d.value));
}

// ════════════════════════════════════════════════════════
// Chart.js でミニ折れ線グラフを描画
// ════════════════════════════════════════════════════════
function drawChart(canvasId, data, color, yLabel) {
  const ctx = document.getElementById(canvasId);
  if (!ctx || data.length < 2) return;

  const labels = data.map(d => d.date);
  const values = data.map(d => d.value);

  new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: color,
        borderWidth: 1.5,
        pointRadius: 0,
        pointHoverRadius: 4,
        pointHoverBackgroundColor: color,
        tension: 0.3,
        fill: true,
        backgroundColor: (ctx) => {
          const c = ctx.chart.ctx;
          const g = c.createLinearGradient(0, 0, 0, 140);
          g.addColorStop(0, color + '28');
          g.addColorStop(1, color + '00');
          return g;
        }
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 600, easing: 'easeInOutQuart' },
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: '#1a1e29',
          borderColor: '#252a38',
          borderWidth: 1,
          titleColor: '#6b7494',
          bodyColor: '#e8ecf4',
          titleFont: { family: 'DM Mono', size: 10 },
          bodyFont: { family: 'DM Mono', size: 12 },
          padding: 10,
          callbacks: {
            title: (items) => items[0].label,
            label: (item) => ' ' + item.raw.toLocaleString()
          }
        }
      },
      scales: {
        x: {
          display: true,
          grid: { display: false },
          afterBuildTicks(scale) {
            const labels = scale.chart.data.labels;
            if (!labels) return;
            const existing = new Set(scale.ticks.map(t => t.value));
            labels.forEach((label, i) => {
              const day = (label || '').slice(8, 10);
              if (day && day !== '01' && !existing.has(i)) {
                scale.ticks.push({ value: i });
                existing.add(i);
              }
            });
            scale.ticks.sort((a, b) => a.value - b.value);
          },
          ticks: {
            color: '#4a5068',
            font: { family: 'DM Mono', size: 9 },
            maxTicksLimit: 6,
            maxRotation: 0,
            callback: function(val, idx) {
              const label = this.getLabelForValue(val);
              if (!label || label.length < 7) return label;
              const day = label.slice(8, 10);
              if (day && day !== '01') {
                const month = parseInt(label.slice(5, 7));
                const weekNum = Math.ceil(parseInt(day) / 7);
                return month + '/' + weekNum + 'W';
              }
              return label.slice(2,4) + '/' + label.slice(5,7);
            }
          },
          border: { color: '#252a38' }
        },
        y: {
          display: true,
          position: 'right',
          grid: {
            display: true,
            color: 'rgba(255,255,255,0.04)',
          },
          ticks: {
            color: '#4a5068',
            font: { family: 'DM Mono', size: 9 },
            maxTicksLimit: 4,
            callback: function(val) {
              // 大きな数字は短縮表示
              if (Math.abs(val) >= 1000000) return (val/1000000).toFixed(1) + 'M';
              if (Math.abs(val) >= 1000) return (val/1000).toFixed(0) + 'k';
              return val.toLocaleString();
            }
          },
          title: {
            display: true,
            text: yLabel || '',
            color: '#4a5068',
            font: { family: 'DM Mono', size: 9 },
            padding: { bottom: 4 }
          },
          border: { color: '#252a38' }
        }
      },
      interaction: { mode: 'nearest', axis: 'x', intersect: false }
    }
  });
}

// ════════════════════════════════════════════════════════
// カード HTML を生成
// ════════════════════════════════════════════════════════
function renderCard(item) {
  return `
    <div class="card" id="card-${item.key}">
      <div class="card-top">
        <div>
          <div class="card-title">${item.label}</div>
          <div class="card-unit">${item.unit}</div>
          <div class="card-status loading" id="status-${item.key}">
            <span class="status-dot"></span>LOADING
          </div>
        </div>
        <div class="card-value-block">
          <div class="card-value" id="val-${item.key}">
            <span class="skeleton skeleton-value"></span>
          </div>
          <div class="card-date" id="date-${item.key}">—</div>
        </div>
      </div>
      <div class="chart-wrap">
        <span class="skeleton skeleton-chart" id="skel-${item.key}"></span>
        <canvas id="chart-${item.key}" style="display:none;"></canvas>
      </div>
      ${item.note ? `<div class="card-note">📌 ${item.note}</div>` : ''}
    </div>
  `;
}

// ════════════════════════════════════════════════════════
// 初期レンダリング
// ════════════════════════════════════════════════════════
function buildDOM() {
  const root = document.getElementById('dashboardRoot');
  let html = '';

  INDICATORS.forEach(cat => {
    html += `
      <div class="category-header">
        <span class="cat-dot"></span>${cat.category}
      </div>
      <div class="grid">
        ${cat.items.map(renderCard).join('')}
      </div>
    `;
  });

  root.innerHTML = html;
}

// ════════════════════════════════════════════════════════
// データ読み込み & カード更新
// ════════════════════════════════════════════════════════
async function loadIndicator(item, jsonData, progressCb) {
  try {
    const data = getSheetData(jsonData, item.sheetName);
    if (data.length === 0) throw new Error('No data');

    const latest = data[data.length - 1];
    const valEl = document.getElementById(`val-${item.key}`);
    const dateEl = document.getElementById(`date-${item.key}`);
    const statusEl = document.getElementById(`status-${item.key}`);
    const skelEl = document.getElementById(`skel-${item.key}`);
    const chartEl = document.getElementById(`chart-${item.key}`);

    // 値の表示（整数なら カンマ区切り、小数なら小数2桁）
    const formatted = Number.isInteger(latest.value)
      ? latest.value.toLocaleString()
      : latest.value.toLocaleString('ja-JP', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

    valEl.textContent = formatted;
    dateEl.textContent = latest.date;
    statusEl.className = 'card-status ok';
    statusEl.innerHTML = '<span class="status-dot static"></span>LIVE';

    // スケルトン非表示 → チャート表示
    skelEl.style.display = 'none';
    chartEl.style.display = 'block';
    drawChart(`chart-${item.key}`, data, item.color, item.unit);

  } catch (e) {
    const statusEl = document.getElementById(`status-${item.key}`);
    const valEl = document.getElementById(`val-${item.key}`);
    const skelEl = document.getElementById(`skel-${item.key}`);
    statusEl.className = 'card-status error';
    statusEl.innerHTML = '<span class="status-dot static"></span>ERROR';
    valEl.textContent = '—';
    if (skelEl) skelEl.style.display = 'none';
  } finally {
    progressCb();
  }
}

// ════════════════════════════════════════════════════════
// メイン
// ════════════════════════════════════════════════════════
async function main() {
  buildDOM();

  const allItems = INDICATORS.flatMap(c => c.items);
  const total = allItems.length;
  let done = 0;

  const bar = document.getElementById('loadingBar');
  const updateTimeEl = document.getElementById('updateTime');

  let jsonData;
  try {
    jsonData = await loadDataJSON();
    updateTimeEl.textContent = (jsonData.generated_at || '').replace(/-/g, '.');
  } catch (e) {
    updateTimeEl.textContent = '—';
    allItems.forEach(item => {
      const statusEl = document.getElementById(`status-${item.key}`);
      const valEl    = document.getElementById(`val-${item.key}`);
      const skelEl   = document.getElementById(`skel-${item.key}`);
      if (statusEl) { statusEl.className = 'card-status error'; statusEl.innerHTML = '<span class="status-dot static"></span>ERROR'; }
      if (valEl) valEl.textContent = '—';
      if (skelEl) skelEl.style.display = 'none';
    });
    bar.style.width = '100%';
    return;
  }

  const progressCb = () => {
    done++;
    bar.style.width = (done / total * 100) + '%';
  };

  // 並列でデータ取得
  await Promise.all(allItems.map(item => loadIndicator(item, jsonData, progressCb)));
}

main();
