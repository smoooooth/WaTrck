<script>
const SAVE_TOKEN_ENDPOINT = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/saveToken';
const WHATSAPP_PHONE = '971585927034';
const LEAD_FORM_CONVERSION_NAME = 'DVT_SLF_Offline';
const WHATSAPP_CONVERSION_NAME = 'DVT_WA_Contact';
const PROJECT_ID = 'DVT_Pagani';


const WA_FORM_TOKEN_KEY = 'wa_form_token';
const WA_FORM_TOKEN_TTL_HOURS = 24;

function generateToken(len = 7) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let out = '';
  const arr = crypto.getRandomValues(new Uint8Array(len));
  for (let i = 0; i < len; i++) out += chars[arr[i] % chars.length];
  return out;
}

function getGclid(ttlHours = 24) {
  try {
    const params = new URLSearchParams(window.location.search);
    const urlGclid = params.get('gclid');
    if (urlGclid) {
      const payload = { gclid: urlGclid, ts: Date.now() };
      try { localStorage.setItem('wa_gclid', JSON.stringify(payload)); } catch {}
      return urlGclid;
    }
    const raw = localStorage.getItem('wa_gclid');
    if (!raw) return '';
    let parsed;
    try { parsed = JSON.parse(raw); } catch (e) { try { localStorage.removeItem('wa_gclid'); } catch {} return ''; }
    if (!parsed.gclid || !parsed.ts) { try { localStorage.removeItem('wa_gclid'); } catch {} return ''; }
    const ageMs = Date.now() - parsed.ts;
    const ttlMs = ttlHours * 3600 * 1000;
    if (ageMs <= ttlMs) return parsed.gclid;
    try { localStorage.removeItem('wa_gclid'); } catch {}
    return '';
  } catch (e) {
    return '';
  }
}


function getCampaignId(ttlHours = 24) {
  try {
    const params = new URLSearchParams(window.location.search);
    const urlCid = params.get('campaign_id');
    if (urlCid) {
      const payload = { cid: urlCid, ts: Date.now() };
      try { localStorage.setItem('wa_campaign_id', JSON.stringify(payload)); } catch {}
      return urlCid;
    }

    const raw = localStorage.getItem('wa_campaign_id');
    if (!raw) return '';
    let parsed;
    try { parsed = JSON.parse(raw); } catch (e) {
      try { localStorage.removeItem('wa_campaign_id'); } catch {}
      return '';
    }
    if (!parsed.cid || !parsed.ts) {
      try { localStorage.removeItem('wa_campaign_id'); } catch {}
      return '';
    }

    const ageMs = Date.now() - parsed.ts;
    const ttlMs = ttlHours * 3600 * 1000;
    if (ageMs <= ttlMs) return parsed.cid;

    try { localStorage.removeItem('wa_campaign_id'); } catch {}
    return '';
  } catch (e) {
    return '';
  }
}

function getCanonicalPage() {
  try {
    const loc = window.location;
    let host = loc.hostname.toLowerCase();
    if (host.startsWith('www.')) host = host.substring(4);
    let path = loc.pathname;
    if (!path) path = '/';
    if (path !== '/' && path.endsWith('/')) path = path.slice(0, -1);
    return (host + path).toLowerCase();
  } catch (e) {
    return '';
  }
}

function trySendBeacon(obj) {
  try {
    if (navigator.sendBeacon) {
      const blob = new Blob([JSON.stringify(obj)], { type: 'application/json' });
      return navigator.sendBeacon(SAVE_TOKEN_ENDPOINT, blob);
    }
  } catch (e) {}
  return false;
}

async function fetchSend(obj) {
  try {
    await fetch(SAVE_TOKEN_ENDPOINT, {
      method: 'POST',
      mode: 'cors',
      credentials: 'omit',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(obj),
      keepalive: true
    });
  } catch (err) {}
}

async function handleWhatsappClick(prefillText, ctaId) {
  const token = generateToken(7);
  const gclid = getGclid(24) || '';
  const campaignId = getCampaignId(24) || '';
  const ts = new Date().toISOString();
  const page = getCanonicalPage();
  const payloadObj = {
    token,
    gclid,
	campaign_id: campaignId,
    ts,
    page,
    projectId: PROJECT_ID,
    ctaId: ctaId,
    lead_value_estimate: 70,
    conversion_name: WHATSAPP_CONVERSION_NAME,
    used: false
  };
  const beaconOk = trySendBeacon(payloadObj);
  if (!beaconOk) await fetchSend(payloadObj);
  const fullText = `${prefillText} Ref: #${token}`;
const waUrl = `https://api.whatsapp.com/send?phone=${WHATSAPP_PHONE}&text=${encodeURIComponent(fullText)}`;


	try {
	  if (typeof gtag_report_conversion === 'function') {
		try { gtag_report_conversion(); } catch (e) {}
	  }
	} catch (e) {}


	const newWin = window.open(waUrl, '_blank');
	if (!newWin || newWin.closed || typeof newWin.closed === 'undefined') {
	  setTimeout(() => { window.location.assign(waUrl); }, 150);
	}


}

async function handleFormSuccessElement(el) {
  try {
    if (el.hasAttribute('data-conversion-tracked')) return;
    el.setAttribute('data-conversion-tracked', 'true');
	
	const tokenFromInput = (el && el.closest && el.closest('form') && el.closest('form').querySelector('input[name="token"]')) ?
						   el.closest('form').querySelector('input[name="token"]').value :
						   null;
	const token = tokenFromInput || getStoredToken() || generateToken(7);
	storeToken(token);
    const gclid = getGclid(24) || '';
    const ts = new Date().toISOString();
	const campaignId = getCampaignId(24) || '';
    const page = getCanonicalPage();
    const payloadObj = {
      token,
      gclid,
	  campaign_id: campaignId,
      ts,
      page,
      projectId: PROJECT_ID,
      ctaId: 'lead_form',
      lead_value_estimate: 70,
      conversion_name: LEAD_FORM_CONVERSION_NAME,
      used: true
    };
    let beaconOk = false;
    try { beaconOk = trySendBeacon(payloadObj); } catch (e) { beaconOk = false; }
    if (beaconOk) {
      fetchSend(payloadObj);
    } else {
      try { await fetchSend(payloadObj); } catch (e) {}
    }
    try {
      if (typeof gtag_report_conversion === 'function') {
        try { gtag_report_conversion(); } catch (e) {}
      }
    } catch (e) {}
  } catch (e) {}
}

function installFormSuccessObserver() {
  const observer = new MutationObserver((mutations) => {
    mutations.forEach((m) => {
      if (m.type === 'attributes' && m.attributeName === 'style') {
        const target = m.target;
        if (target.classList && target.classList.contains('success-message') && target.classList.contains('w-form-done')) {
          try { handleFormSuccessElement(target); } catch (e) {}
        }
      }
      if (m.type === 'childList' && m.addedNodes && m.addedNodes.length) {
        m.addedNodes.forEach(node => {
          if (node.nodeType === 1 && node.classList && node.classList.contains('success-message') && node.classList.contains('w-form-done')) {
            try { handleFormSuccessElement(node); } catch (e) {}
          }
        });
      }
    });
  });

  const existing = document.querySelectorAll('.success-message.w-form-done');
  existing.forEach(el => {
    try {
      observer.observe(el, { attributes: true, attributeFilter: ['style'] });
      if (window.getComputedStyle(el).display === 'block') handleFormSuccessElement(el);
    } catch (e) {}
  });

  try { observer.observe(document.body, { childList: true, subtree: true }); } catch (e) {}
}




function getStoredToken() {
  try {
    const raw = localStorage.getItem(WA_FORM_TOKEN_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw);
    if (!obj || !obj.token || !obj.ts) return null;
    const ageMs = Date.now() - obj.ts;
    if (ageMs > WA_FORM_TOKEN_TTL_HOURS * 3600 * 1000) {
      localStorage.removeItem(WA_FORM_TOKEN_KEY);
      return null;
    }
    return obj.token;
  } catch (e) {
    try { localStorage.removeItem(WA_FORM_TOKEN_KEY); } catch (__) {}
    return null;
  }
}

function storeToken(token) {
  try {
    localStorage.setItem(WA_FORM_TOKEN_KEY, JSON.stringify({ token: token, ts: Date.now() }));
  } catch (e) {}
}


function ensureTokenExists() {
  let token = getStoredToken();
  if (!token) {
    token = generateToken(7); 
    storeToken(token);
  }
  return token;
}


function ensureTokenFieldOnForm(formEl) {
  if (!formEl || formEl.nodeType !== 1) return;
  let input = formEl.querySelector('input[name="token"]');
  const token = ensureTokenExists();
  if (!input) {
    input = document.createElement('input');
    input.type = 'hidden';
    input.name = 'token';
    input.className = 'wa-token-field';
    input.value = token;
    formEl.appendChild(input);
  } else {
    input.value = token;
  }
}


function initWaFormToken() {
  const forms = Array.from(document.forms || document.querySelectorAll('form'));
  const token = ensureTokenExists();
  forms.forEach(form => {
    ensureTokenFieldOnForm(form);

    form.addEventListener('submit', function () {
      try { ensureTokenFieldOnForm(form); } catch (e) {}
    }, true);
  });

  const mo = new MutationObserver(muts => {
    muts.forEach(m => {
      m.addedNodes && m.addedNodes.forEach(node => {
        if (node.nodeType === 1) {
          if (node.tagName === 'FORM') ensureTokenFieldOnForm(node);
          node.querySelectorAll && node.querySelectorAll('form').forEach(f => ensureTokenFieldOnForm(f));
        }
      });
    });
  });
  try { mo.observe(document.body, { childList: true, subtree: true }); } catch (e) {}
}

// call on DOM ready
document.addEventListener('DOMContentLoaded', function() {
  initWaFormToken();
  installFormSuccessObserver();
});




document.getElementById('hero_sec_cta')?.addEventListener('click', function (e) {
  e.preventDefault();
  handleWhatsappClick("Hello, I would like to book a private tour of DaVinci Tower by Pagani.", "hero_sec_cta");
});

document.getElementById('sticky_bottom_bar_CTA')?.addEventListener('click', function (e) {
  e.preventDefault();
  handleWhatsappClick("Hello, I would like to know more about DaVinci Tower by Pagani.", "sticky_bottom_bar_CTA");
});

document.querySelectorAll('#intro_slider_brochure_cta').forEach(btn => {
  btn.addEventListener('click', function (e) {
    e.preventDefault();
    handleWhatsappClick(
      "Hello, I would like to request DaVinci Tower by Pagani brochure.",
      "intro_slider_brochure_cta"
    );
  });
});


document.getElementById('navbar_wa_icon')?.addEventListener('click', function (e) {
  e.preventDefault();
  handleWhatsappClick("Hello, I would like to know more about DaVinci Tower by Pagani.", "navbar_wa_icon");
});

document.querySelectorAll('#request_layouts_cta').forEach(btn => {
  btn.addEventListener('click', function (e) {
    e.preventDefault();
    handleWhatsappClick(
      "Hello, I would like to request the price list for DaVinci Tower by Pagani.",
      "request_layouts_cta"
    );
  });
});


</script>