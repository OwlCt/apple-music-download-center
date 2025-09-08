const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const state = {
  meta: null,
  metaType: null, // 'album' | 'artist' | null
  artistChosenAlbums: [],
};

function isAlbumUrl(u) { return /\/album\//.test(u); }
function isArtistUrl(u) { return /\/artist\//.test(u); }

async function fetchJSON(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function renderMeta() {
  const box = $('#meta');
  box.innerHTML = '';
  state.artistChosenAlbums = [];
  $('#createTasksFromArtist').disabled = true;

  if (!state.meta) return;

  if (state.metaType === 'album') {
    const { title, artist, tracks } = state.meta;
    const h = document.createElement('div');
    h.innerHTML = `
      <h3>专辑曲目（可多选）</h3>
      <div class="muted">${artist} - ${title}</div>
      <div class="form-row">
        <button id="selectAll" class="secondary">全选</button>
        <button id="selectNone" class="secondary">清空</button>
      </div>
      <div class="list" id="trackList"></div>
    `;
    box.appendChild(h);
    const list = h.querySelector('#trackList');
    tracks.forEach(t => {
      const item = document.createElement('div');
      item.className = 'item';
      item.innerHTML = `<label><input type="checkbox" class="track" value="${t.index}" checked> ${String(t.index).padStart(2,'0')}. ${t.name}</label>`;
      list.appendChild(item);
    });
    h.querySelector('#selectAll').onclick = () => $$('.track').forEach(cb => cb.checked = true);
    h.querySelector('#selectNone').onclick = () => $$('.track').forEach(cb => cb.checked = false);
  }

  if (state.metaType === 'artist') {
    const { albums } = state.meta;
    const h = document.createElement('div');
    h.innerHTML = `
      <h3>艺术家专辑（勾选需要下载的专辑）</h3>
      <div class="list" id="albumList"></div>
    `;
    box.appendChild(h);
    const list = h.querySelector('#albumList');
    albums.forEach(a => {
      const item = document.createElement('div');
      item.className = 'item';
      item.innerHTML = `<label><input type="checkbox" class="albumPick" data-url="${a.url}"> ${a.name} <span class="muted">(${a.date || ''})</span></label>`;
      list.appendChild(item);
    });
    list.addEventListener('change', () => {
      state.artistChosenAlbums = $$('.albumPick').filter(x => x.checked).map(x => x.dataset.url);
      $('#createTasksFromArtist').disabled = state.artistChosenAlbums.length === 0;
    });
  }
}

async function onFetchMeta() {
  const url = $('#url').value.trim();
  if (!url) return;
  try {
    if (isAlbumUrl(url)) {
      const meta = await fetchJSON(`/v1/meta/album?url=${encodeURIComponent(url)}`);
      state.meta = meta;
      state.metaType = 'album';
      renderMeta();
      return;
    }
    if (isArtistUrl(url)) {
      const meta = await fetchJSON(`/v1/meta/artist?url=${encodeURIComponent(url)}`);
      state.meta = meta;
      state.metaType = 'artist';
      renderMeta();
      return;
    }
    // 其他类型无需预取
    state.meta = null;
    state.metaType = null;
    renderMeta();
  } catch (e) {
    alert('获取信息失败: ' + e.message);
  }
}

async function createTask() {
  const url = $('#url').value.trim();
  const quality = $('#quality').value;
  if (!url) return alert('请输入链接');
  const body = { quality };
  if (state.metaType === 'album') {
    const selected = $$('.track').filter(x => x.checked).map(x => parseInt(x.value, 10));
    body.urls = [url];
    if (selected.length > 0 && selected.length !== (state.meta?.tracks?.length || 0)) {
      body.tracks = selected;
    }
  } else {
    body.urls = [url];
  }
  try {
    await fetchJSON('/v1/tasks', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    await refreshTasks();
  } catch (e) {
    alert('创建任务失败: ' + e.message);
  }
}

async function createTasksFromArtist() {
  const quality = $('#quality').value;
  if (state.artistChosenAlbums.length === 0) return;
  try {
    await fetchJSON('/v1/tasks', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ quality, urls: state.artistChosenAlbums }) });
    await refreshTasks();
  } catch (e) {
    alert('创建任务失败: ' + e.message);
  }
}

function renderTasks(list) {
  const box = $('#tasks');
  box.innerHTML = '';
  if (!list || list.length === 0) {
    box.innerHTML = '<div class="muted">暂无任务</div>';
    return;
  }
  list.forEach(t => {
    const el = document.createElement('div');
    el.className = 'task';
    el.innerHTML = `
      <div class="url" title="${t.url}">${t.url}</div>
      <div>${t.quality}</div>
      <div>${t.status}</div>
      <div>${t.progress || 0}%</div>
      <div class="progress"><div class="bar" style="width:${t.progress || 0}%"></div></div>
      <div style="display:flex;gap:6px;justify-content:flex-end">
        <button class="secondary" data-id="${t.id}" data-action="detail">详情</button>
        <button class="secondary" data-id="${t.id}" data-action="retry" ${t.status === 'running' ? 'disabled' : ''}>重试</button>
      </div>
    `;
    box.appendChild(el);
  });

  box.onclick = async (e) => {
    const btn = e.target.closest('button');
    if (!btn) return;
    const id = btn.dataset.id;
    const action = btn.dataset.action;
    if (action === 'retry') {
      try {
        await fetchJSON(`/v1/tasks/${id}/retry`, { method: 'POST' });
        await refreshTasks();
      } catch (e) {
        alert('重试失败: ' + e.message);
      }
    }
    if (action === 'detail') {
      try {
        const t = await fetchJSON(`/v1/tasks/${id}`);
        alert(`状态: ${t.status}\n进度: ${t.progress}%\n日志:\n- ` + (t.logs || []).join('\n- '));
      } catch (e) {
        alert('获取详情失败: ' + e.message);
      }
    }
  };
}

async function refreshTasks() {
  try {
    const list = await fetchJSON('/v1/tasks');
    renderTasks(list);
  } catch (e) {
    console.error(e);
  }
}

function init() {
  $('#fetchMeta').onclick = onFetchMeta;
  $('#createTask').onclick = createTask;
  $('#createTasksFromArtist').onclick = createTasksFromArtist;
  $('#refreshTasks').onclick = refreshTasks;
  refreshTasks();
  setInterval(refreshTasks, 2000);
}

document.addEventListener('DOMContentLoaded', init);

