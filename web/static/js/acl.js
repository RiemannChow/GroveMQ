// GroveMQ ACL (Authorization) Dashboard JavaScript
class GroveMQACL {
    constructor() {
        this.refreshInterval = 5000; // 5 seconds
        this.intervalId = null;
        this.authToken = null;
        this.init();
    }

    init() {
        this.checkAuthentication();
        this.bindEvents();
        if (this.authToken) {
            this.refreshData();
            this.startAutoRefresh();
        }
    }

    checkAuthentication() {
        this.authToken = localStorage.getItem('emqx_token');
        if (!this.authToken && !window.location.pathname.includes('/login')) {
            window.location.href = '/login';
        }
    }

    bindEvents() {
        const refreshBtn = document.getElementById('refresh-btn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshData());
        }

        const autoRefreshToggle = document.getElementById('auto-refresh');
        if (autoRefreshToggle) {
            autoRefreshToggle.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.startAutoRefresh();
                } else {
                    this.stopAutoRefresh();
                }
            });
        }

        const logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn) {
            logoutBtn.addEventListener('click', () => this.handleLogout());
        }

        const newRuleBtn = document.getElementById('new-rule-btn');
        if (newRuleBtn) {
            newRuleBtn.addEventListener('click', () => this.openModal(null));
        }

        const modalClose = document.getElementById('modal-close');
        if (modalClose) {
            modalClose.addEventListener('click', () => this.closeModal());
        }
        const modalCancel = document.getElementById('modal-cancel');
        if (modalCancel) {
            modalCancel.addEventListener('click', () => this.closeModal());
        }
        const modalSave = document.getElementById('modal-save');
        if (modalSave) {
            modalSave.addEventListener('click', () => this.saveRule());
        }

        // Delegated actions for table rows.
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('acl-toggle-btn')) {
                const id = e.target.dataset.id;
                const enabled = e.target.dataset.enabled === 'true';
                this.toggleRule(id, !enabled);
            }
            if (e.target.classList.contains('acl-edit-btn')) {
                const id = e.target.dataset.id;
                this.editRule(id);
            }
            if (e.target.classList.contains('acl-delete-btn')) {
                const id = e.target.dataset.id;
                this.deleteRule(id);
            }
        });
    }

    startAutoRefresh() {
        this.stopAutoRefresh();
        this.intervalId = setInterval(() => this.refreshData(), this.refreshInterval);
    }

    stopAutoRefresh() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
    }

    handleLogout() {
        localStorage.removeItem('emqx_token');
        window.location.href = '/login';
    }

    apiHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        if (this.authToken) {
            headers['Authorization'] = 'Bearer ' + this.authToken;
        }
        return headers;
    }

    refreshData() {
        this.loadStats();
        this.loadRules();
        const t = document.getElementById('last-refresh-time');
        if (t) {
            t.textContent = new Date().toLocaleTimeString();
        }
    }

    loadRules() {
        fetch('/api/acl', { headers: this.apiHeaders() })
            .then((r) => r.json())
            .then((resp) => {
                const data = resp.data || [];
                this.renderRules(data);
            })
            .catch((err) => {
                console.error('Failed to load ACL rules:', err);
            });
    }

    renderRules(rules) {
        const body = document.getElementById('acl-table-body');
        if (!body) return;

        if (!rules || rules.length === 0) {
            body.innerHTML = '<tr><td colspan="9" style="text-align: center; color: #8c8c8c; padding: 24px;">No ACL rules configured</td></tr>';
            return;
        }

        body.innerHTML = '';
        rules.forEach((rule) => {
            const tr = document.createElement('tr');
            const permBadge = rule.permission === 'deny'
                ? '<span class="badge-deny">deny</span>'
                : '<span class="badge-allow">allow</span>';
            const enabledBadge = rule.enabled
                ? '<span class="badge-enabled">enabled</span>'
                : '<span class="badge-disabled">disabled</span>';
            const toggleLabel = rule.enabled ? 'Disable' : 'Enable';
            tr.innerHTML = `
                <td>${this.esc(rule.id || '')}</td>
                <td>${permBadge}</td>
                <td>${this.esc(rule.action || 'all')}</td>
                <td>${this.esc(rule.username || '*')}</td>
                <td>${this.esc(rule.clientid || '*')}</td>
                <td>${this.esc(rule.topic || '*')}</td>
                <td>${rule.priority || 0}</td>
                <td>${enabledBadge}</td>
                <td>
                    <button class="btn btn-small acl-toggle-btn" data-id="${this.esc(rule.id)}" data-enabled="${rule.enabled}">${toggleLabel}</button>
                    <button class="btn btn-small acl-edit-btn" data-id="${this.esc(rule.id)}">Edit</button>
                    <button class="btn btn-small acl-delete-btn" data-id="${this.esc(rule.id)}">Delete</button>
                </td>`;
            body.appendChild(tr);
        });
    }

    loadStats() {
        fetch('/api/acl/stats', { headers: this.apiHeaders() })
            .then((r) => r.json())
            .then((resp) => {
                const data = resp.data || {};
                const set = (id, val) => {
                    const el = document.getElementById(id);
                    if (el) el.textContent = val;
                };
                set('stat-total', data.total_rules || 0);
                set('stat-enabled', data.enabled_rules || 0);
                set('stat-allow', data.allow_rules || 0);
                set('stat-deny', data.deny_rules || 0);
                const denials = (data.recent_denials || []).length;
                set('stat-denials', denials);
                set('stat-match', data.match_count || 0);
            })
            .catch((err) => {
                console.error('Failed to load ACL stats:', err);
            });
    }

    openModal(rule) {
        const modal = document.getElementById('rule-modal');
        const title = document.getElementById('modal-title');
        if (!modal) return;

        if (rule) {
            title.textContent = 'Edit ACL Rule';
            document.getElementById('rule-id').value = rule.id || '';
            document.getElementById('rule-permission').value = rule.permission || 'allow';
            document.getElementById('rule-action').value = rule.action || 'all';
            document.getElementById('rule-username').value = rule.username || '';
            document.getElementById('rule-username-pattern').value = rule.username_pattern || '';
            document.getElementById('rule-clientid').value = rule.clientid || '';
            document.getElementById('rule-clientid-pattern').value = rule.clientid_pattern || '';
            document.getElementById('rule-topic').value = rule.topic || '';
            document.getElementById('rule-priority').value = rule.priority || 100;
            document.getElementById('rule-description').value = rule.description || '';
            document.getElementById('rule-enabled').checked = rule.enabled !== false;
        } else {
            title.textContent = 'New ACL Rule';
            document.getElementById('rule-id').value = '';
            document.getElementById('rule-permission').value = 'allow';
            document.getElementById('rule-action').value = 'all';
            document.getElementById('rule-username').value = '';
            document.getElementById('rule-username-pattern').value = '';
            document.getElementById('rule-clientid').value = '';
            document.getElementById('rule-clientid-pattern').value = '';
            document.getElementById('rule-topic').value = '';
            document.getElementById('rule-priority').value = 100;
            document.getElementById('rule-description').value = '';
            document.getElementById('rule-enabled').checked = true;
        }
        document.getElementById('rule-error').textContent = '';
        modal.style.display = 'flex';
    }

    closeModal() {
        const modal = document.getElementById('rule-modal');
        if (modal) modal.style.display = 'none';
    }

    saveRule() {
        const id = document.getElementById('rule-id').value;
        const body = {
            permission: document.getElementById('rule-permission').value,
            action: document.getElementById('rule-action').value,
            username: document.getElementById('rule-username').value,
            username_pattern: document.getElementById('rule-username-pattern').value,
            clientid: document.getElementById('rule-clientid').value,
            clientid_pattern: document.getElementById('rule-clientid-pattern').value,
            topic: document.getElementById('rule-topic').value,
            priority: parseInt(document.getElementById('rule-priority').value, 10) || 0,
            description: document.getElementById('rule-description').value,
            enabled: document.getElementById('rule-enabled').checked,
        };

        const errEl = document.getElementById('rule-error');
        const url = id ? '/api/acl/' + encodeURIComponent(id) : '/api/acl';
        const method = id ? 'PUT' : 'POST';

        fetch(url, {
            method: method,
            headers: this.apiHeaders(),
            body: JSON.stringify(body),
        })
            .then((r) => {
                if (!r.ok) return r.json().then((e) => Promise.reject(e));
                return r.json();
            })
            .then(() => {
                this.closeModal();
                this.refreshData();
            })
            .catch((err) => {
                errEl.textContent = (err && err.message) || 'Failed to save rule';
            });
    }

    editRule(id) {
        fetch('/api/acl/' + encodeURIComponent(id), { headers: this.apiHeaders() })
            .then((r) => r.json())
            .then((resp) => {
                this.openModal(resp.data || null);
            })
            .catch((err) => {
                console.error('Failed to load rule:', err);
            });
    }

    toggleRule(id, enabled) {
        fetch('/api/acl/' + encodeURIComponent(id), {
            method: 'PUT',
            headers: this.apiHeaders(),
            body: JSON.stringify({ enabled: enabled }),
        })
            .then(() => this.refreshData())
            .catch((err) => console.error('Failed to toggle rule:', err));
    }

    deleteRule(id) {
        if (!confirm('Delete ACL rule ' + id + '?')) return;
        fetch('/api/acl/' + encodeURIComponent(id), {
            method: 'DELETE',
            headers: this.apiHeaders(),
        })
            .then(() => this.refreshData())
            .catch((err) => console.error('Failed to delete rule:', err));
    }

    esc(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new GroveMQACL();
});
