const fs = require('fs');
const http = require('http');
const { spawn } = require('child_process');

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

function postJson(url, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const postData = JSON.stringify(body);
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port,
        path: u.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postData),
        },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

function getAirflowCookie() {
  return new Promise((resolve, reject) => {
    http.get('http://localhost:8080/login/', (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => {
        const m = data.match(/name="csrf_token"[^>]*value="([^"]+)"/);
        if (!m) return resolve(null);
        const csrf = m[1];
        const postData = `username=sismowatch_admin&password=sw_airflow_admin_9812&csrf_token=${encodeURIComponent(
          csrf
        )}`;
        const req = http.request(
          {
            hostname: 'localhost',
            port: 8080,
            path: '/login/',
            method: 'POST',
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
              'Content-Length': Buffer.byteLength(postData),
              Cookie: res.headers['set-cookie'] ? res.headers['set-cookie'][0].split(';')[0] : '',
            },
          },
          (loginRes) => {
            const setCookie = loginRes.headers['set-cookie'];
            if (setCookie) {
              const sess = setCookie.find((c) => c.startsWith('session='));
              if (sess) {
                const val = sess.split(';')[0].replace('session=', '');
                return resolve(val);
              }
            }
            resolve(null);
          }
        );
        req.on('error', reject);
        req.write(postData);
        req.end();
      });
    }).on('error', reject);
  });
}

function getMinioToken() {
  return new Promise((resolve) => {
    const postData = JSON.stringify({
      accessKey: 'sismowatch_minio',
      secretKey: 'sw_minio_local_9812',
    });
    const req = http.request(
      {
        hostname: 'localhost',
        port: 9001,
        path: '/api/v1/login',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postData),
        },
      },
      (res) => {
        const setCookie = res.headers['set-cookie'];
        if (setCookie) {
          const tok = setCookie.find((c) => c.startsWith('token='));
          if (tok) {
            const val = tok.split(';')[0].replace('token=', '');
            return resolve(val);
          }
        }
        resolve(null);
      }
    );
    req.on('error', () => resolve(null));
    req.write(postData);
    req.end();
  });
}

class CDPClient {
  constructor(wsUrl) {
    this.ws = new WebSocket(wsUrl);
    this.id = 0;
    this.pending = new Map();
    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) reject(msg.error);
        else resolve(msg.result);
      }
    };
  }

  async ready() {
    if (this.ws.readyState === WebSocket.OPEN) return;
    return new Promise((resolve) => {
      this.ws.onopen = () => resolve();
    });
  }

  send(method, params = {}) {
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  close() {
    this.ws.close();
  }
}

async function main() {
  fs.mkdirSync('/home/cohorte5/IAriwi/docs/img', { recursive: true });

  console.log('1. Obteniendo credenciales y tokens de sesión...');
  const loginRes = await postJson('http://localhost:3000/auth/login', {
    username: 'admin',
    password: 'AdminSismo2026!',
  });
  const session = {
    token: loginRes.access_token,
    username: loginRes.user.username,
    role: loginRes.user.role,
  };
  const airflowSessionCookie = await getAirflowCookie();
  const minioToken = await getMinioToken();

  console.log('2. Iniciando Google Chrome headless...');
  const chrome = spawn('google-chrome', [
    '--headless=new',
    '--remote-debugging-port=9222',
    '--no-sandbox',
    '--disable-gpu',
    '--window-size=1600,1050',
    'about:blank',
  ]);

  await sleep(1500);

  try {
    const pages = await fetchJson('http://127.0.0.1:9222/json/list');
    const targetPage = pages.find((p) => p.type === 'page') || pages[0];
    const cdp = new CDPClient(targetPage.webSocketDebuggerUrl);
    await cdp.ready();

    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');
    await cdp.send('DOM.enable');
    await cdp.send('Network.enable');

    // 1. Dashboard
    console.log('3. Capturando 01_dashboard_principal.png...');
    await cdp.send('Page.navigate', { url: 'http://localhost:3000' });
    await sleep(2000);
    await cdp.send('Runtime.evaluate', {
      expression: `
        localStorage.setItem('sismowatch_session_v1', JSON.stringify(${JSON.stringify(session)}));
        sessionStorage.setItem('sismowatch_session_v1', JSON.stringify(${JSON.stringify(session)}));
        location.reload();
      `,
    });
    await sleep(4500);
    let shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync('/home/cohorte5/IAriwi/docs/img/01_dashboard_principal.png', Buffer.from(shot.data, 'base64'));

    // 2. Chat IA
    console.log('4. Capturando 02_asistente_ia_analitico.png...');
    await cdp.send('Runtime.evaluate', {
      expression: `
        const btns = Array.from(document.querySelectorAll('button'));
        const chatBtn = btns.find(b => b.textContent && b.textContent.includes('Asistente'));
        if (chatBtn) chatBtn.click();
      `,
    });
    await sleep(2500);
    await cdp.send('Runtime.evaluate', {
      expression: `
        const sugBtns = Array.from(document.querySelectorAll('button'));
        const qBtn = sugBtns.find(b => b.textContent && b.textContent.includes('sismo más fuerte'));
        if (qBtn) qBtn.click();
      `,
    });
    await sleep(4000);
    shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync('/home/cohorte5/IAriwi/docs/img/02_asistente_ia_analitico.png', Buffer.from(shot.data, 'base64'));

    // 3. OpenMetadata
    console.log('5. Capturando 03_openmetadata_gobernanza.png...');
    await cdp.send('Page.navigate', { url: 'http://localhost:8585' });
    await sleep(4000);
    await cdp.send('Runtime.evaluate', {
      expression: `
        const emailInput = document.querySelector('input[type="email"]') || document.querySelector('input[id*="email"]');
        const passInput = document.querySelector('input[type="password"]');
        const submitBtn = document.querySelector('button[type="submit"]') || document.querySelector('button.ant-btn-primary');
        if (emailInput && passInput && submitBtn) {
          emailInput.value = 'admin@open-metadata.org';
          emailInput.dispatchEvent(new Event('input', { bubbles: true }));
          passInput.value = 'admin';
          passInput.dispatchEvent(new Event('input', { bubbles: true }));
          submitBtn.click();
        }
      `,
    });
    await sleep(4500);
    shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync('/home/cohorte5/IAriwi/docs/img/03_openmetadata_gobernanza.png', Buffer.from(shot.data, 'base64'));

    // 4. Airflow
    console.log('6. Capturando 04_airflow_orquestacion.png...');
    if (airflowSessionCookie) {
      await cdp.send('Network.setCookie', {
        name: 'session',
        value: airflowSessionCookie,
        domain: 'localhost',
        path: '/',
      });
      await cdp.send('Page.navigate', { url: 'http://localhost:8080/home' });
      await sleep(4000);
      shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync('/home/cohorte5/IAriwi/docs/img/04_airflow_orquestacion.png', Buffer.from(shot.data, 'base64'));
    }

    // 5. MinIO
    console.log('7. Capturando 05_minio_datalake.png...');
    if (minioToken) {
      await cdp.send('Network.setCookie', {
        name: 'token',
        value: minioToken,
        domain: 'localhost',
        path: '/',
      });
      await cdp.send('Page.navigate', { url: 'http://localhost:9001/buckets' });
      await sleep(3500);
      shot = await cdp.send('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync('/home/cohorte5/IAriwi/docs/img/05_minio_datalake.png', Buffer.from(shot.data, 'base64'));
    }

    cdp.close();
    console.log('Capturas generadas exitosamente en docs/img/');
  } finally {
    chrome.kill();
  }
}

main().catch((err) => {
  console.error('Error en capturas:', err);
  process.exit(1);
});

