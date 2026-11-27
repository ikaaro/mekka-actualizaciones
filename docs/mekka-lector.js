/*
 * mekka-lector.js  [drive-v1]
 * Abre un respaldo .mekka (zip con socios.db y usuarios.db) dentro del navegador, sin subirlo a ningún lado.
 * Trae un lector de SQLite de solo lectura (tablas normales) y un lector de zip que usa la descompresión
 * del propio navegador. No depende de nada externo.
 *
 *   const r = await MekkaLector.abrir(arrayBuffer)
 *   r.manifest            -> { fecha, ... }
 *   r.db('socios.db')     -> base: .tablas(), .filas('socios') -> [{col: valor, ...}]
 */
(function (global) {
  'use strict';

  // ------------------------------------------------------------------ zip
  async function inflar(datos) {
    const ds = new DecompressionStream('deflate-raw');
    const flujo = new Blob([datos]).stream().pipeThrough(ds);
    return new Uint8Array(await new Response(flujo).arrayBuffer());
  }

  async function leerZip(buf) {
    const u8 = new Uint8Array(buf), dv = new DataView(buf);
    let fin = -1;
    for (let i = u8.length - 22; i >= Math.max(0, u8.length - 65557); i--) {
      if (dv.getUint32(i, true) === 0x06054b50) { fin = i; break; }
    }
    if (fin < 0) throw new Error('El archivo no es un respaldo de Mekka (no es un zip).');
    const n = dv.getUint16(fin + 10, true);
    let p = dv.getUint32(fin + 16, true);
    const archivos = {};
    const dec = new TextDecoder();
    for (let k = 0; k < n; k++) {
      if (dv.getUint32(p, true) !== 0x02014b50) throw new Error('El respaldo está dañado.');
      const metodo = dv.getUint16(p + 10, true);
      const tamComp = dv.getUint32(p + 20, true);
      const lnom = dv.getUint16(p + 28, true), lext = dv.getUint16(p + 30, true), lcom = dv.getUint16(p + 32, true);
      const loc = dv.getUint32(p + 42, true);
      const nombre = dec.decode(u8.subarray(p + 46, p + 46 + lnom));
      const ini = loc + 30 + dv.getUint16(loc + 26, true) + dv.getUint16(loc + 28, true);
      const crudo = u8.subarray(ini, ini + tamComp);
      archivos[nombre] = { metodo, crudo };
      p += 46 + lnom + lext + lcom;
    }
    return {
      nombres: Object.keys(archivos),
      async leer(nombre) {
        const a = archivos[nombre];
        if (!a) return null;
        if (a.metodo === 0) return a.crudo.slice();
        if (a.metodo === 8) return inflar(a.crudo);
        throw new Error('Formato de compresión no soportado.');
      },
    };
  }

  // ------------------------------------------------------------------ SQLite (solo lectura)
  function BaseSQLite(u8) {
    if (new TextDecoder().decode(u8.subarray(0, 15)) !== 'SQLite format 3') throw new Error('La base del respaldo está dañada.');
    const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
    let tamPag = dv.getUint16(16); if (tamPag === 1) tamPag = 65536;
    const usable = tamPag - u8[20];
    const dec = new TextDecoder('utf-8');

    function varint(pos) {
      let v = 0;
      for (let i = 0; i < 8; i++) {
        const b = u8[pos + i];
        v = v * 128 + (b & 0x7f);
        if (!(b & 0x80)) return [v, i + 1];
      }
      return [v * 256 + u8[pos + 8], 9];
    }

    function payload(pos, total) {
      const X = usable - 35;
      if (total <= X) return u8.subarray(pos, pos + total);
      const M = Math.floor((usable - 12) * 32 / 255) - 23;
      let K = M + ((total - M) % (usable - 4));
      const local = K <= X ? K : M;
      const out = new Uint8Array(total);
      out.set(u8.subarray(pos, pos + local), 0);
      let hecho = local, sig = dv.getUint32(pos + local);
      while (hecho < total && sig) {
        const base = (sig - 1) * tamPag;
        const n = Math.min(usable - 4, total - hecho);
        out.set(u8.subarray(base + 4, base + 4 + n), hecho);
        hecho += n;
        sig = dv.getUint32(base);
      }
      return out;
    }

    function registro(b) {
      const v = new DataView(b.buffer, b.byteOffset, b.byteLength);
      let [tamCab, i] = varintDe(b, 0);
      const tipos = [];
      while (i < tamCab) { const [t, n] = varintDe(b, i); tipos.push(t); i += n; }
      let p = tamCab;
      return tipos.map(t => {
        switch (t) {
          case 0: return null;
          case 1: p += 1; return v.getInt8(p - 1);
          case 2: p += 2; return v.getInt16(p - 2);
          case 3: p += 3; return ((v.getInt8(p - 3) << 16) | (b[p - 2] << 8) | b[p - 1]);
          case 4: p += 4; return v.getInt32(p - 4);
          case 5: p += 6; return v.getInt16(p - 6) * 4294967296 + v.getUint32(p - 4);
          case 6: p += 8; return Number(v.getBigInt64(p - 8));
          case 7: p += 8; return v.getFloat64(p - 8);
          case 8: return 0;
          case 9: return 1;
          default:
            if (t >= 12 && t % 2 === 0) { const n = (t - 12) / 2; p += n; return b.subarray(p - n, p); }
            if (t >= 13) { const n = (t - 13) / 2; p += n; return dec.decode(b.subarray(p - n, p)); }
            return null;
        }
      });
    }
    function varintDe(b, pos) {
      let v = 0;
      for (let i = 0; i < 8; i++) { const x = b[pos + i]; v = v * 128 + (x & 0x7f); if (!(x & 0x80)) return [v, i + 1]; }
      return [v * 256 + b[pos + 8], 9];
    }

    // recorre una tabla (b-tree de rowid) y llama fn(rowid, valores)
    function recorrer(raiz, fn) {
      const pila = [raiz];
      while (pila.length) {
        const pag = pila.pop();
        const base = (pag - 1) * tamPag, cab = pag === 1 ? 100 : 0;
        const tipo = u8[base + cab];
        const n = dv.getUint16(base + cab + 3);
        if (tipo === 0x0d) {
          for (let i = 0; i < n; i++) {
            let c = base + dv.getUint16(base + cab + 8 + i * 2);
            const [tam, a] = varint(c); c += a;
            const [rowid, b2] = varint(c); c += b2;
            fn(rowid, registro(payload(c, tam)));
          }
        } else if (tipo === 0x05) {
          const hijos = [];
          for (let i = 0; i < n; i++) hijos.push(dv.getUint32(base + dv.getUint16(base + cab + 12 + i * 2)));
          hijos.push(dv.getUint32(base + cab + 8));
          for (let i = hijos.length - 1; i >= 0; i--) pila.push(hijos[i]);
        } else {
          throw new Error('Página desconocida en la base.');
        }
      }
    }

    // columnas a partir del CREATE TABLE (incluye las que se agregaron con ALTER TABLE)
    function columnas(sql) {
      const a = sql.indexOf('('), z = sql.lastIndexOf(')');
      const cuerpo = sql.slice(a + 1, z);
      const partes = []; let nivel = 0, act = '', comilla = null;
      for (const ch of cuerpo) {
        if (comilla) { act += ch; if (ch === comilla) comilla = null; continue; }
        if (ch === "'" || ch === '"' || ch === '`' || ch === '[') { comilla = ch === '[' ? ']' : ch; act += ch; continue; }
        if (ch === '(') nivel++;
        if (ch === ')') nivel--;
        if (ch === ',' && nivel === 0) { partes.push(act.trim()); act = ''; continue; }
        act += ch;
      }
      if (act.trim()) partes.push(act.trim());
      const cols = [];
      for (const p of partes) {
        if (/^(PRIMARY|UNIQUE|CHECK|FOREIGN|CONSTRAINT)\b/i.test(p)) continue;
        const m = p.match(/^("([^"]+)"|`([^`]+)`|\[([^\]]+)\]|(\S+))\s*(.*)$/s);
        const nombre = m[2] || m[3] || m[4] || m[5];
        const resto = m[6] || '';
        cols.push({ nombre, rowid: /^INTEGER\s+PRIMARY\s+KEY/i.test(resto.trim()) });
      }
      return cols;
    }

    const esquema = {};
    recorrer(1, (_r, v) => {
      if (v[0] === 'table' && v[4] && !/^sqlite_/.test(v[1])) esquema[v[1]] = { raiz: v[3], cols: columnas(v[4]) };
    });

    return {
      tablas: () => Object.keys(esquema),
      tiene: t => !!esquema[t],
      columnas: t => esquema[t] ? esquema[t].cols.map(c => c.nombre) : [],
      filas(t) {
        const e = esquema[t];
        if (!e) return [];
        const out = [];
        recorrer(e.raiz, (rowid, v) => {
          const o = {};
          e.cols.forEach((c, i) => { o[c.nombre] = c.rowid ? rowid : (i < v.length ? v[i] : null); });
          out.push(o);
        });
        return out;
      },
    };
  }

  async function abrir(buf) {
    const zip = await leerZip(buf);
    const m = await zip.leer('manifest.json');
    if (!m) throw new Error('Este archivo no es un respaldo de Mekka.');
    const manifest = JSON.parse(new TextDecoder().decode(m));
    if (manifest.formato !== 'mekka-respaldo') throw new Error('Este archivo no es un respaldo de Mekka.');
    const bases = {};
    for (const n of ['socios.db', 'usuarios.db']) {
      const d = await zip.leer(n);
      if (d) bases[n] = BaseSQLite(d);
    }
    if (!bases['socios.db']) throw new Error('El respaldo no trae la base de socios.');
    return { manifest, db: n => bases[n] || null };
  }

  global.MekkaLector = { abrir, leerZip, BaseSQLite };
})(typeof window !== 'undefined' ? window : globalThis);
