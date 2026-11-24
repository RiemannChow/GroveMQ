// A2A Registry dashboard page.
(function() {
    'use strict';

    const refreshBtn = document.getElementById('refresh-btn');
    const notice = document.getElementById('a2a-notice');
    const tbody = document.querySelector('#a2a-agents-table tbody');

    function setNotice(message, kind) {
        if (!notice) return;
        if (!message) {
            notice.style.display = 'none';
            notice.textContent = '';
            return;
        }
        notice.style.display = 'block';
        notice.textContent = message;
        notice.className = kind === 'error' ? 'card card-error' : 'card';
    }

    function fetchData(url) {
        return fetch(url, { headers: { 'Accept': 'application/json' } })
            .then(function(response) {
                if (response.status === 501) {
                    return { disabled: true };
                }
                if (!response.ok) {
                    throw new Error('HTTP ' + response.status);
                }
                return response.json();
            })
            .then(function(envelope) {
                if (envelope && envelope.disabled) return envelope;
                if (!envelope || envelope.code !== 0) {
                    throw new Error((envelope && envelope.message) || 'invalid API response');
                }
                return { data: envelope.data };
            });
    }

    function textCell(row, value) {
        const td = document.createElement('td');
        td.textContent = value === undefined || value === null ? '' : String(value);
        row.appendChild(td);
        return td;
    }

    function renderEmpty(message) {
        if (!tbody) return;
        tbody.textContent = '';
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 8;
        td.style.textAlign = 'center';
        td.style.color = '#666';
        td.style.padding = '2rem';
        td.textContent = message;
        tr.appendChild(td);
        tbody.appendChild(tr);
    }

    function renderAgents(agents) {
        if (!tbody) return;
        tbody.textContent = '';
        if (!agents || agents.length === 0) {
            renderEmpty('No A2A agents registered');
            return;
        }

        agents.forEach(function(agent) {
            const tr = document.createElement('tr');

            textCell(tr, agent.address);

            const card = agent.card || {};
            textCell(tr, card.name || '');
            textCell(tr, card.version || '');

            const statusTd = document.createElement('td');
            const status = document.createElement('span');
            status.className = 'status ' + (agent.status === 'online' ? 'online' : 'offline');
            status.textContent = agent.status || '-';
            statusTd.appendChild(status);
            tr.appendChild(statusTd);

            textCell(tr, agent.status_source);
            textCell(tr, Array.isArray(card.skills)
                ? card.skills.map(function(skill) { return skill.name || skill.id; }).join(', ')
                : '');
            textCell(tr, agent.updated_at);

            const actions = document.createElement('td');
            const button = document.createElement('button');
            button.className = 'btn btn-danger btn-small';
            button.textContent = 'Unregister';
            button.addEventListener('click', function() {
                if (!window.confirm('Unregister ' + agent.address + '?')) return;
                fetch('/api/v5/a2a/agents/' + encodeURIComponent(agent.address), { method: 'DELETE' })
                    .then(function(response) {
                        if (!response.ok) throw new Error('HTTP ' + response.status);
                        return response.json();
                    })
                    .then(function(envelope) {
                        if (!envelope || envelope.code !== 0) {
                            throw new Error((envelope && envelope.message) || 'unregister failed');
                        }
                        loadAll();
                    })
                    .catch(function(error) {
                        setNotice('Failed to unregister agent: ' + error.message, 'error');
                    });
            });
            actions.appendChild(button);
            tr.appendChild(actions);
            tbody.appendChild(tr);
        });
    }

    function renderStats(stats) {
        const values = {
            'a2a-registered': stats.registered,
            'a2a-online': stats.online,
            'a2a-offline': stats.offline,
            'a2a-validation-failures': stats.validation_failures
        };
        Object.keys(values).forEach(function(id) {
            const el = document.getElementById(id);
            if (el) el.textContent = values[id] === undefined ? '-' : String(values[id]);
        });
    }

    function loadAll() {
        fetchData('/api/v5/a2a/stats')
            .then(function(result) {
                if (result.disabled) {
                    setNotice('A2A Registry is disabled. Enable broker.a2a_registry.enabled and restart the broker.', 'error');
                    renderEmpty('A2A Registry is disabled');
                    return;
                }
                setNotice('');
                renderStats(result.data || {});
            })
            .catch(function(error) {
                setNotice('Failed to load A2A Registry stats: ' + error.message, 'error');
            });

        fetchData('/api/v5/a2a/agents')
            .then(function(result) {
                if (result.disabled) return;
                renderAgents(result.data || []);
            })
            .catch(function(error) {
                setNotice('Failed to load A2A agents: ' + error.message, 'error');
            });
    }

    if (refreshBtn) refreshBtn.addEventListener('click', loadAll);
    loadAll();
    setInterval(loadAll, 15000);
})();
