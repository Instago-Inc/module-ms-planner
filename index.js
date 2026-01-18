(function () {
  const graph = require('graph@latest');
  const msauth = require('msauth@latest');
  const msAuthConfig = require('msauth-config@latest');
  const log = require('log@latest').create('ms-planner');

  const cfg = {
    planId: null,
    bucketId: null,
    bucketName: null,
    debug: false,
    auth: null,
    defaultAssignees: [],
    resolveUsers: false
  };

  function configure(opts) {
    if (!opts || typeof opts !== 'object') return;
    if (opts.planId) cfg.planId = String(opts.planId);
    if (opts.bucketId) cfg.bucketId = String(opts.bucketId);
    if (opts.bucketName) cfg.bucketName = String(opts.bucketName);
    if (typeof opts.debug === 'boolean') cfg.debug = opts.debug;
    if (opts.auth && typeof opts.auth === 'object') cfg.auth = opts.auth;
    if (Array.isArray(opts.defaultAssignees)) cfg.defaultAssignees = opts.defaultAssignees.slice();
    if (typeof opts.resolveUsers === 'boolean') cfg.resolveUsers = opts.resolveUsers;
  }

  function parseList(val) {
    if (!val) return [];
    if (Array.isArray(val)) return val.map((v) => String(v)).filter((v) => v);
    if (typeof val === 'string') {
      return val
        .split(/[;,]/)
        .map((v) => String(v).trim())
        .filter((v) => v);
    }
    return [];
  }

  function envGet(primary, legacy) {
    const v = sys.env.get(primary);
    if (v !== undefined && v !== null && String(v) !== '') return v;
    if (legacy) return sys.env.get(legacy);
    return v;
  }

  function envFlag(primary, legacy) {
    const v = envGet(primary, legacy);
    if (v === undefined || v === null || String(v) === '') return null;
    const s = String(v).toLowerCase();
    return s === '1' || s === 'true' || s === 'yes';
  }

  function isGuid(val) {
    return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(String(val || ''));
  }

  function parseDateTime(input) {
    if (!input) return null;
    const d = new Date(input);
    if (isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  function parseDueDate(task) {
    if (!task) return null;
    const raw = task.dueDateTime;
    if (!raw) return null;
    if (typeof raw === 'string') {
      const d = new Date(raw);
      return isNaN(d.getTime()) ? null : d;
    }
    if (typeof raw === 'object' && raw.dateTime) {
      const d = new Date(raw.dateTime);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  }

  function pickPriority(val) {
    if (val === 0 || val === 1 || val === 2 || val === 3 || val === 4 || val === 5 || val === 6 || val === 7 || val === 8 || val === 9 || val === 10) {
      return val;
    }
    const s = String(val || '').toLowerCase();
    if (s === 'urgent') return 0;
    if (s === 'important') return 1;
    if (s === 'medium') return 5;
    if (s === 'low') return 9;
    return null;
  }

  function parsePlanIdFromLink(value) {
    if (!value) return '';
    const raw = String(value);
    const decode = (input) => {
      try { return decodeURIComponent(input); } catch { return input; }
    };
    const pickPlanId = (input) => {
      if (!input) return '';
      const match = String(input).match(/\/plan\/([A-Za-z0-9_-]+)/i);
      return match ? match[1] : '';
    };
    const contextMatch = raw.match(/context=([^&]+)/i);
    if (contextMatch && contextMatch[1]) {
      const idFromContext = pickPlanId(decode(contextMatch[1]));
      if (idFromContext) return idFromContext;
    }
    const urlMatch = raw.match(/webUrl=([^&]+)/i);
    if (urlMatch && urlMatch[1]) {
      const idFromWeb = pickPlanId(decode(urlMatch[1]));
      if (idFromWeb) return idFromWeb;
    }
    return pickPlanId(raw);
  }

  function resolvePlanId(opts) {
    const planLink = (opts && opts.planLink) || sys.env.get('ms-planner.planLink');
    const planId = (opts && opts.planId)
      || cfg.planId
      || envGet('ms-planner.planId', 'planner.planId')
      || (planLink ? parsePlanIdFromLink(planLink) : '');
    return { planId, planLink };
  }

  async function resolveBucketId(overrides, auth, debug) {
    const link = (overrides && overrides.planLink) || sys.env.get('ms-planner.planLink');
    const bucketId =
      (overrides && overrides.bucketId) ||
      cfg.bucketId ||
      envGet('ms-planner.bucketId', 'planner.bucketId');
    if (bucketId) return { ok: true, data: String(bucketId) };

    const planId = (overrides && overrides.planId)
      || cfg.planId
      || envGet('ms-planner.planId', 'planner.planId')
      || (link ? parsePlanIdFromLink(link) : '');
    if (!planId) return { ok: false, error: 'ms-planner: planId is required to resolve bucket' };

    const bucketName =
      (overrides && overrides.bucketName) ||
      cfg.bucketName ||
      envGet('ms-planner.bucket', 'planner.bucket');

    const res = await graph.json({
      path: `planner/plans/${encodeURIComponent(planId)}/buckets`,
      auth,
      debug
    });

    if (!res || !res.ok) {
      return { ok: false, error: (res && res.error) || 'ms-planner: failed to list buckets', status: res && res.status };
    }

    const items = (res.data && res.data.value) || [];
    if (!items.length) return { ok: false, error: 'ms-planner: no buckets found for plan' };

    if (bucketName) {
      const hit = items.find((b) => b && b.name === bucketName);
      if (hit && hit.id) return { ok: true, data: hit.id };
      return { ok: false, error: 'ms-planner: bucket not found for name ' + bucketName };
    }

    return { ok: true, data: items[0].id };
  }

  async function resolveUserId(identifier, auth, debug) {
    const res = await graph.json({
      path: `users/${encodeURIComponent(identifier)}?$select=id`,
      auth,
      debug
    });
    if (!res || !res.ok || !res.data || !res.data.id) {
      return { ok: false, error: (res && res.error) || 'ms-planner: failed to resolve user ' + identifier };
    }
    return { ok: true, data: res.data.id };
  }

  async function buildAssignments(list, auth, debug, resolveUsers) {
    const assignments = {};
    const errors = [];
    for (let i = 0; i < list.length; i++) {
      const raw = list[i];
      if (!raw) continue;
      const identifier = String(raw);
      let userId = identifier;
      const shouldResolve = resolveUsers || identifier.indexOf('@') >= 0;
      if (shouldResolve && !isGuid(identifier)) {
        const res = await resolveUserId(identifier, auth, debug);
        if (!res.ok) {
          errors.push(res.error || 'ms-planner: failed to resolve user ' + identifier);
          continue;
        }
        userId = res.data;
      }
      assignments[userId] = {
        '@odata.type': '#microsoft.graph.plannerAssignment',
        orderHint: ' !'
      };
    }
    return { ok: errors.length === 0, data: assignments, error: errors.join('; ') };
  }

  function normalizeLabels(input) {
    const list = Array.isArray(input) ? input : (input ? [input] : []);
    const out = [];
    const seen = new Set();
    for (const item of list) {
      const raw = String(item || '').trim();
      if (!raw) continue;
      const key = raw.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(raw);
    }
    return out;
  }

  async function ensurePlanCategories(planId, labels, auth, debug) {
    const list = normalizeLabels(labels);
    if (!list.length) return { applied: null };
    const detailsRes = await graph.json({
      path: `planner/plans/${encodeURIComponent(String(planId))}/details`,
      auth,
      debug
    });
    if (!detailsRes || !detailsRes.ok || !detailsRes.data) {
      return { applied: null, error: (detailsRes && detailsRes.error) || 'ms-planner: failed to fetch plan details' };
    }
    const etag = detailsRes.data['@odata.etag'];
    const current = detailsRes.data.categoryDescriptions || {};
    const slotByLabel = {};
    const freeSlots = [];
    for (let i = 1; i <= 25; i += 1) {
      const key = `category${i}`;
      const name = current[key];
      if (name && typeof name === 'string') {
        slotByLabel[name.toLowerCase()] = key;
      } else {
        freeSlots.push(key);
      }
    }
    let mutated = false;
    for (const label of list) {
      const key = slotByLabel[label.toLowerCase()];
      if (key) continue;
      if (!freeSlots.length) continue;
      const slot = freeSlots.shift();
      current[slot] = label;
      slotByLabel[label.toLowerCase()] = slot;
      mutated = true;
    }
    if (mutated && etag) {
      const patchRes = await graph.json({
        path: `planner/plans/${encodeURIComponent(String(planId))}/details`,
        method: 'PATCH',
        headers: { 'If-Match': etag },
        bodyObj: { categoryDescriptions: current },
        auth,
        debug
      });
      if (!patchRes || !patchRes.ok) {
        return { applied: null, error: (patchRes && patchRes.error) || 'ms-planner: failed to update plan categories' };
      }
    }
    const applied = {};
    for (const label of list) {
      const slot = slotByLabel[label.toLowerCase()];
      if (slot) applied[slot] = true;
    }
    return { applied };
  }

  function mergeReferences(existing, list) {
    const refs = (existing && typeof existing === 'object') ? Object.assign({}, existing) : {};
    const items = Array.isArray(list) ? list : (list ? [list] : []);
    for (const entry of items) {
      if (!entry) continue;
      const url = String(entry.url || entry).trim();
      if (!url) continue;
      const key = url
        .replace(/%/g, '%25')
        .replace(/\./g, '%2E')
        .replace(/:/g, '%3A')
        .replace(/@/g, '%40')
        .replace(/#/g, '%23');
      refs[key] = Object.assign(
        { '@odata.type': '#microsoft.graph.plannerExternalReference', type: 'Other' },
        entry && typeof entry === 'object' ? entry : {},
        { alias: entry && entry.alias ? String(entry.alias) : 'GitHub' }
      );
      if ('url' in refs[key]) delete refs[key].url;
    }
    return refs;
  }

  async function updateTaskDetails(taskId, description, references, auth, debug) {
    if (!description && !references) return { ok: true };
    const detailsPath = `planner/tasks/${encodeURIComponent(String(taskId))}/details`;
    const detailsRes = await graph.json({
      path: detailsPath,
      auth,
      debug
    });
    if (!detailsRes || !detailsRes.ok || !detailsRes.data) {
      return { ok: false, error: (detailsRes && detailsRes.error) || 'ms-planner: failed to fetch task details' };
    }
    const etag = detailsRes.data['@odata.etag'];
    if (!etag) return { ok: false, error: 'ms-planner: missing details etag' };
    const mergedRefs = references ? mergeReferences(detailsRes.data.references, references) : undefined;
    const patchDetails = async (bodyObj) => {
      return graph.json({
        path: `planner/tasks/${encodeURIComponent(String(taskId))}/details`,
        method: 'PATCH',
        headers: { 'If-Match': etag },
        bodyObj,
        auth,
        debug
      });
    };
    let res = await patchDetails({
      ...(description ? { description: String(description) } : {}),
      ...(mergedRefs ? { references: mergedRefs } : {})
    });
    if (!res || !res.ok) {
      const msg = res && res.error ? String(res.error) : '';
      const wantsRetry = description && mergedRefs && msg.toLowerCase().includes('instance annotation');
      if (wantsRetry) {
        res = await patchDetails({ description: String(description) });
      }
    }
    if (!res || !res.ok) {
      return { ok: false, error: (res && res.error) || 'ms-planner: failed to update task details' };
    }
    return { ok: true };
  }

  async function ensureAuth(inputAuth) {
    if (inputAuth && typeof inputAuth === 'object') return inputAuth;
    const res = await msauth.ensureAuthenticated(msAuthConfig.load({ scope: 'planner' }));
    if (res && res.status === 'ok' && res.tokens) {
      const cfg = msAuthConfig.load({ scope: 'planner' });
      return {
        accessToken: res.tokens.access_token,
        refreshToken: res.tokens.refresh_token,
        tenant: cfg.tenant,
        clientId: cfg.clientId,
        clientSecret: cfg.clientSecret,
        scope: cfg.scope
      };
    }
      if (res && res.status === 'pending' && res.device) {
        const verifyUrl = res.device.verification_uri || 'https://microsoft.com/devicelogin';
        const userCode = res.device.user_code || '';
        log.error('auth pending: verify at', verifyUrl, 'code', userCode);
        throw new Error('ms-planner: auth pending, please complete device login');
      }
    throw new Error('ms-planner: auth failed');
  }

  async function createTask(opts) {
    try {
      if (!opts || typeof opts !== 'object') return { ok: false, error: 'ms-planner.createTask: options required' };
      if (!opts.title) return { ok: false, error: 'ms-planner.createTask: title is required' };

      const auth = await ensureAuth(opts.auth || cfg.auth);
      const debug = typeof opts.debug === 'boolean' ? opts.debug : cfg.debug;
      const planInfo = resolvePlanId(opts);
      const planLink = planInfo.planLink;
      const planId = planInfo.planId;
      if (!planId) return { ok: false, error: 'ms-planner.createTask: planId is required' };

      const bucketRes = await resolveBucketId(
        { planId, planLink, bucketId: opts.bucketId, bucketName: opts.bucketName },
        auth,
        debug
      );
      if (!bucketRes.ok) return { ok: false, error: bucketRes.error };

      const assignmentsList = parseList(opts.assignTo || opts.assignees || cfg.defaultAssignees || envGet('ms-planner.assignTo', 'planner.assignTo'));
      const resolveUsers = typeof opts.resolveUsers === 'boolean'
        ? opts.resolveUsers
        : (typeof cfg.resolveUsers === 'boolean' ? cfg.resolveUsers : (envFlag('ms-planner.resolveUsers', 'planner.resolveUsers') === true));
      const assignmentsRes = assignmentsList.length
        ? await buildAssignments(assignmentsList, auth, debug, resolveUsers)
        : { ok: true, data: {} };
      if (!assignmentsRes.ok && !Object.keys(assignmentsRes.data || {}).length) {
        return { ok: false, error: assignmentsRes.error || 'ms-planner.createTask: failed to resolve assignees' };
      }

      const payload = {
        planId: String(planId),
        bucketId: String(bucketRes.data),
        title: String(opts.title)
      };

      if (assignmentsRes.data && Object.keys(assignmentsRes.data).length) {
        payload.assignments = assignmentsRes.data;
      }

      const dueDateTime = parseDateTime(opts.dueDate || opts.dueDateTime);
      if (dueDateTime) payload.dueDateTime = dueDateTime;

      const startDateTime = parseDateTime(opts.startDate || opts.startDateTime);
      if (startDateTime) payload.startDateTime = startDateTime;

      const priority = pickPriority(opts.priority);
      if (priority !== null) payload.priority = priority;

      const appliedRes = await ensurePlanCategories(planId, opts.labels || opts.categories, auth, debug);
      if (appliedRes && appliedRes.error) {
        return { ok: false, error: appliedRes.error };
      }
      if (appliedRes && appliedRes.applied) payload.appliedCategories = appliedRes.applied;

      const res = await graph.json({
        path: 'planner/tasks',
        method: 'POST',
        bodyObj: payload,
        auth,
        debug
      });

      if (!res || !res.ok) {
        return { ok: false, error: (res && res.error) || 'ms-planner.createTask: request failed', status: res && res.status };
      }

      const taskId = res && res.data && res.data.id ? res.data.id : '';
      const detailsRes = taskId
        ? await updateTaskDetails(taskId, opts.description, opts.references, auth, debug)
        : { ok: true };
      if (!detailsRes.ok) {
        return { ok: false, error: detailsRes.error || 'ms-planner.createTask: failed to update details' };
      }

      return { ok: true, data: res.data, status: res.status, error: assignmentsRes.error || undefined };
    } catch (e) {
      log.error('createTask:error', (e && (e.message || e)) || 'unknown');
      return { ok: false, error: (e && (e.message || String(e))) || 'unknown' };
    }
  }

  async function listTasks(opts) {
    try {
      const input = (opts && typeof opts === 'object') ? opts : {};
      const auth = await ensureAuth(input.auth || cfg.auth);
      const debug = typeof input.debug === 'boolean' ? input.debug : cfg.debug;
      const resolveUsers = typeof input.resolveUsers === 'boolean'
        ? input.resolveUsers
        : (typeof cfg.resolveUsers === 'boolean' ? cfg.resolveUsers : (envFlag('ms-planner.resolveUsers', 'planner.resolveUsers') === true));
      const planInfo = resolvePlanId(input);
      const planLink = planInfo.planLink;
      const planId = planInfo.planId;
      if (!planId) return { ok: false, error: 'ms-planner.listTasks: planId is required' };

      let path = `planner/plans/${encodeURIComponent(planId)}/tasks`;
      if (input.bucketId || input.bucketName || cfg.bucketId || cfg.bucketName || envGet('ms-planner.bucket', 'planner.bucket')) {
        const bucketRes = await resolveBucketId(
          { planId, planLink, bucketId: input.bucketId, bucketName: input.bucketName },
          auth,
          debug
        );
        if (!bucketRes.ok) return { ok: false, error: bucketRes.error };
        path = `planner/buckets/${encodeURIComponent(String(bucketRes.data))}/tasks`;
      }

      const query = [];
      if (input.top) query.push(`$top=${encodeURIComponent(String(input.top))}`);
      const url = query.length ? `${path}?${query.join('&')}` : path;
      const res = await graph.json({ path: url, auth, debug });
      if (!res || !res.ok) {
        return { ok: false, error: (res && res.error) || 'ms-planner.listTasks: request failed', status: res && res.status };
      }
      const list = (res.data && res.data.value) ? res.data.value : [];
      let filtered = list.slice();

      if (input.unassigned === true) {
        filtered = filtered.filter((task) => !task.assignments || Object.keys(task.assignments).length === 0);
      }

      if (input.assignedTo) {
        const identifier = String(input.assignedTo || '');
        let userId = identifier;
        const shouldResolve = resolveUsers || identifier.indexOf('@') >= 0;
        if (shouldResolve && !isGuid(identifier)) {
          const resUser = await resolveUserId(identifier, auth, debug);
          if (!resUser.ok) {
            return { ok: false, error: resUser.error || 'ms-planner.listTasks: failed to resolve assignee' };
          }
          userId = resUser.data;
        }
        filtered = filtered.filter((task) => task.assignments && Object.prototype.hasOwnProperty.call(task.assignments, userId));
      }

      if (input.dueBefore || input.dueAfter) {
        const before = input.dueBefore ? new Date(input.dueBefore) : null;
        const after = input.dueAfter ? new Date(input.dueAfter) : null;
        filtered = filtered.filter((task) => {
          const due = parseDueDate(task);
          if (!due) return false;
          if (before && !isNaN(before.getTime()) && due > before) return false;
          if (after && !isNaN(after.getTime()) && due < after) return false;
          return true;
        });
      }

      return { ok: true, data: Object.assign({}, res.data, { value: filtered }), status: res.status };
    } catch (e) {
      log.error('listTasks:error', (e && (e.message || e)) || 'unknown');
      return { ok: false, error: (e && (e.message || String(e))) || 'unknown' };
    }
  }

  async function assignTask(opts) {
    try {
      if (!opts || typeof opts !== 'object') return { ok: false, error: 'ms-planner.assignTask: options required' };
      if (!opts.taskId) return { ok: false, error: 'ms-planner.assignTask: taskId is required' };

      const auth = await ensureAuth(opts.auth || cfg.auth);
      const debug = typeof opts.debug === 'boolean' ? opts.debug : cfg.debug;
      const assignmentsList = parseList(opts.assignTo || opts.assignees || cfg.defaultAssignees || envGet('ms-planner.assignTo', 'planner.assignTo'));
      if (!assignmentsList.length) return { ok: false, error: 'ms-planner.assignTask: assignees required' };

      const resolveUsers = typeof opts.resolveUsers === 'boolean'
        ? opts.resolveUsers
        : (typeof cfg.resolveUsers === 'boolean' ? cfg.resolveUsers : (envFlag('ms-planner.resolveUsers', 'planner.resolveUsers') === true));
      const assignmentsRes = await buildAssignments(assignmentsList, auth, debug, resolveUsers);
      if (!assignmentsRes.ok && !Object.keys(assignmentsRes.data || {}).length) {
        return { ok: false, error: assignmentsRes.error || 'ms-planner.assignTask: failed to resolve assignees' };
      }

      const taskRes = await graph.json({
        path: `planner/tasks/${encodeURIComponent(String(opts.taskId))}`,
        auth,
        debug
      });
      if (!taskRes || !taskRes.ok || !taskRes.data) {
        return { ok: false, error: (taskRes && taskRes.error) || 'ms-planner.assignTask: failed to fetch task' };
      }

      const etag = taskRes.data['@odata.etag'];
      if (!etag) return { ok: false, error: 'ms-planner.assignTask: missing task etag' };

      const existing = (taskRes.data.assignments && typeof taskRes.data.assignments === 'object')
        ? taskRes.data.assignments
        : {};
      const merged = Object.assign({}, existing, assignmentsRes.data);

      const res = await graph.json({
        path: `planner/tasks/${encodeURIComponent(String(opts.taskId))}`,
        method: 'PATCH',
        headers: { 'If-Match': etag },
        bodyObj: { assignments: merged },
        auth,
        debug
      });

      if (!res || !res.ok) {
        return { ok: false, error: (res && res.error) || 'ms-planner.assignTask: request failed', status: res && res.status };
      }

      return { ok: true, data: res.data, status: res.status, error: assignmentsRes.error || undefined };
    } catch (e) {
      log.error('assignTask:error', (e && (e.message || e)) || 'unknown');
      return { ok: false, error: (e && (e.message || String(e))) || 'unknown' };
    }
  }

  module.exports = { configure, createTask, listTasks, assignTask };
})();
