(() => {
  const el = document.getElementById('weather-outlook'); if (!el) return;
  const meta = document.getElementById('outlook-meta'), alerts = document.getElementById('weather-alerts');
  const charts = document.getElementById('weather-charts'), values = document.getElementById('weather-values');
  let data = null, horizon = 'hourly';
  const node = (tag, text, cls) => {const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
  const numeric = v => typeof v==='number' && Number.isFinite(v);
  const num = v => numeric(v) ? v.toLocaleString('ru-RU',{maximumFractionDigits:1}) : '—';
  function label(ts) {return new Date(ts*1000).toLocaleString('ru-RU',{timeZone:data.timezone||'UTC',...(horizon==='daily'?{day:'numeric',month:'short'}:{hour:'2-digit',minute:'2-digit'})});}
  const groups = {
    hourly: [
      ['Температура воздуха','°C',[['temperature_2m','Температура','#d75c40']]],
      ['Осадки и вероятность','мм / %',[['precipitation','Осадки, мм','#2988c7'],['precipitation_probability','Вероятность, %','#9a79bf',true]]],
      ['Ветер и порывы','м/с',[['wind_speed_10m','Ветер','#167f7b'],['wind_gusts_10m','Порывы','#aa5174']]],
      ['Влажность воздуха','%',[['relative_humidity_2m','Влажность','#2988c7']]],
      ['Дефицит давления пара','кПа',[['vapour_pressure_deficit','VPD','#bf8430']]],
      ['Поверхность почвы','°C',[['soil_temperature_0cm','Температура, 0 см','#837052']]]
    ],
    daily: [
      ['Температура: минимум и максимум','°C',[['temperature_2m_min','Минимум','#2988c7'],['temperature_2m_max','Максимум','#d75c40']]],
      ['Осадки и вероятность','мм / %',[['precipitation_sum','Осадки, мм','#2988c7'],['precipitation_probability_max','Вероятность, %','#9a79bf',true]]],
      ['Ветер и порывы','м/с',[['wind_speed_10m_max','Ветер','#167f7b'],['wind_gusts_10m_max','Порывы','#aa5174']]],
      ['Эталонное испарение ET₀','мм/сут',[['et0_fao_evapotranspiration','ET₀','#bf8430']]],
      ['УФ-индекс','индекс',[['uv_index_max','Максимум UV','#9a79bf']]]
    ]
  };
  function chart(rows, title, unit, series) {
    const card=node('section',undefined,'weather-chart card');card.append(node('h2',title),node('p',unit,'muted small'));
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 500 180');svg.setAttribute('role','img');svg.setAttribute('aria-label',title+'; точные значения в таблице ниже');
    const add=(tag,attrs,text)=>{const e=document.createElementNS(svg.namespaceURI,tag);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;svg.append(e);return e;};
    const nums=rows.flatMap(r=>series.filter(s=>!s[3]).map(s=>r[s[0]])).filter(numeric);
    let lo=nums.length?Math.min(...nums):0, hi=nums.length?Math.max(...nums):1;
    if(lo===hi){lo-=1;hi+=1;}
    for(let i=0;i<3;i++){const y=16+i*60;add('line',{x1:45,x2:455,y1:y,y2:y,stroke:'#dde3ea'});add('text',{x:2,y:y+4,'font-size':11,fill:'#6b7a8c'},num(hi-i*(hi-lo)/2));}
    const x=i=>45+i*410/Math.max(rows.length-1,1);
    for(const [key,name,color,right]of series){
      let path='';for(let i=0;i<rows.length;i++){const v=rows[i][key];if(!numeric(v)){path+=' ';continue;}const y=136-120*(right?v/100:(v-lo)/(hi-lo));const prev=i>0&&numeric(rows[i-1][key]);path+=(prev?' L':' M')+x(i)+' '+y;const dot=add('circle',{cx:x(i),cy:y,r:3,fill:color});const t=document.createElementNS(svg.namespaceURI,'title');t.textContent=label(rows[i].time)+' · '+name+': '+num(v);dot.append(t);}
      add('path',{d:path,fill:'none',stroke:color,'stroke-width':2.5});
    }
    for(const i of [...new Set([0,Math.floor((rows.length-1)/2),rows.length-1])].filter(i=>i>=0))add('text',{x:x(i),y:166,'text-anchor':'middle','font-size':11,fill:'#6b7a8c'},label(rows[i].time));
    if(series.some(s=>s[3]))for(const [y,v]of [[16,'100%'],[136,'0%']])add('text',{x:463,y,'font-size':10,fill:'#9a79bf'},v);
    card.append(svg);const legend=node('div',undefined,'weather-legend');for(const s of series){const item=node('span',s[1]);item.style.color=s[2];legend.append(item);}card.append(legend);
    if(!nums.length)card.append(node('p','Данные отсутствуют','muted'));return card;
  }
  function render() {
    if(!data)return;charts.replaceChildren();values.replaceChildren();alerts.replaceChildren();
    meta.textContent=`${data.latitude}, ${data.longitude} · ${data.timezone} · `+(data.fetched_at?'обновлено '+new Date(data.fetched_at).toLocaleString('ru-RU'): 'нет сохранённого прогноза');
    if(data.status!=='fresh')alerts.append(node('p',data.status==='disabled'?'Погодный модуль выключен':data.status==='stale'?'Прогноз устарел или неполон. Не используйте его как актуальный.':'Прогноз пока недоступен. Проверьте настройки и обновите погоду.', 'weather-notice warning'));
    for(const a of data.advisories||[])alerts.append(node('p',a.text,'weather-notice '+a.kind));
    const rows=data[horizon]||[];if(!rows.length)return;
    for(const group of groups[horizon])charts.append(chart(rows,...group));
    const table=node('table'),thead=node('thead'),head=node('tr');const fields=groups[horizon].flatMap(g=>g[2]).filter((s,i,a)=>a.findIndex(v=>v[0]===s[0])===i);
    head.append(node('th',horizon==='daily'?'Дата':'Час'));for(const f of fields)head.append(node('th',f[1]));
    if(horizon==='hourly'){head.append(node('th','Направление, °'),node('th','Влажность почвы 0–1 см, м³/м³'));}
    thead.append(head);table.append(thead);const body=node('tbody');for(const r of rows){const tr=node('tr');tr.append(node('td',label(r.time)));for(const f of fields)tr.append(node('td',num(r[f[0]])));if(horizon==='hourly')tr.append(node('td',num(r.wind_direction_10m)),node('td',numeric(r.soil_moisture_0_to_1cm)?r.soil_moisture_0_to_1cm.toFixed(3):'—'));body.append(tr);}table.append(body);values.append(table);
  }
  el.querySelectorAll('[data-horizon]').forEach(b=>b.onclick=()=>{horizon=b.dataset.horizon;el.querySelectorAll('[data-horizon]').forEach(v=>v.setAttribute('aria-pressed',String(v===b)));render();});
  async function load(){try{const r=await fetch('/api/v1/weather/outlook',{cache:'no-store',signal:AbortSignal.timeout(8000)});if(!r.ok)throw Error();data=await r.json();render();}catch{meta.textContent='Не удалось получить прогноз. Данные на экране могли устареть.';alerts.replaceChildren(node('p','Нет связи с сервером погоды','weather-notice warning'));}finally{setTimeout(load,60000);}}
  load();
})();
