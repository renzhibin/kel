const { createApp } = Vue;

createApp({
    data() {
        return {
            currentView: 'monitor',
            monitorSub: 'overview',
            opsSub: 'execute',
            overviewDays: 1,
            stats: {
                total: 0,
                running: 0,
                success: 0,
                failed: 0
            },
            tasks: [],
            recentTasks: [],
            taskFilter: {
                jobCode: '',
                status: ''
            },
            taskPagination: {
                page: 0,
                size: 20,
                total: 0
            },
            selectedTask: null,
            taskProgress: null,
            taskStatistics: null,
            taskManifest: null,
            selectedJobConfigKey: '',
            loadSourceBatch: '',
            jobResult: null,
            diskPath: '/data',
            diskInfo: null,
            autoRefresh: null,
            configKeys: [],
            tableExportConfigKeys: [],
            tableLoadConfigKeys: [],
            exportableTables: [],
            loadableTables: [],
            tableExport: { configKey: '', tableName: '' },
            tableExportResult: null,
            tableLoad: { configKey: '', tableName: '', sourceBatch: '' },
            tableLoadResult: null,
            manualExports: [],
            jobConfigList: [],
            jobConfigModal: {
                isEdit: false,
                configKey: '',
                contentYaml: ''
            },
            jobConfigError: null,
            configSpecHtml: '',
            configSpecLoading: false,
            configSpecError: null
        };
    },
    mounted() {
        this.loadStats();
        this.loadRecentTasks();
        this.loadTasks();

        // 解析 hash 中的 jobCode（如 #monitor/taskList?jobCode=xxx）
        this.applyHashJobCode();

        // 自动刷新（每5秒）
        this.autoRefresh = setInterval(() => {
            if (this.currentView === 'monitor') {
                if (this.monitorSub === 'overview') {
                    this.loadStats();
                    this.loadRecentTasks();
                } else if (this.monitorSub === 'taskList') {
                    this.loadTasks();
                }
            } else if (this.currentView === 'config') {
                this.loadJobConfigs();
            } else if (this.currentView === 'ops') {
                if (this.opsSub === 'tableExport') this.loadManualExports();
            }
        }, 5000);
    },
    beforeUnmount() {
        if (this.autoRefresh) {
            clearInterval(this.autoRefresh);
        }
    },
    computed: {
        executableConfigKeys() {
            return (this.configKeys || []).filter(k => k !== '__global__');
        },
        manifestJson() {
            if (!this.taskManifest) return '';
            try {
                return JSON.stringify(this.taskManifest, null, 2);
            } catch (e) {
                return String(this.taskManifest);
            }
        }
    },
    methods: {
        async loadStats() {
            try {
                const days = this.currentView === 'monitor' && this.monitorSub === 'overview' ? this.overviewDays : 0;
                const response = await axios.get('/api/tasks/stats', { params: days > 0 ? { days } : {} });
                this.stats = response.data;
            } catch (error) {
                console.error('加载统计信息失败:', error);
            }
        },

        async loadRecentTasks() {
            try {
                const days = this.currentView === 'monitor' && this.monitorSub === 'overview' ? this.overviewDays : 0;
                const params = { page: 0, size: 10 };
                if (days > 0) params.days = days;
                const response = await axios.get('/api/tasks', { params });
                this.recentTasks = response.data.data || [];
            } catch (error) {
                console.error('加载最近任务失败:', error);
            }
        },

        async loadTasks() {
            try {
                const params = {
                    page: this.taskPagination.page,
                    size: this.taskPagination.size
                };
                if (this.taskFilter.jobCode) params.jobCode = this.taskFilter.jobCode;
                if (this.taskFilter.status) params.status = this.taskFilter.status;

                const response = await axios.get('/api/tasks', { params });
                this.tasks = response.data.data || [];
                this.taskPagination.total = response.data.total || 0;
            } catch (error) {
                console.error('加载任务列表失败:', error);
                alert('加载任务列表失败: ' + (error.response?.data?.error || error.message));
            }
        },

        resetTaskFilter() {
            this.taskFilter.jobCode = '';
            this.taskFilter.status = '';
            this.taskPagination.page = 0;
            this.loadTasks();
        },

        navTo(main, sub) {
            this.currentView = main;
            if (main === 'monitor') this.monitorSub = sub;
            if (main === 'ops') this.opsSub = sub;
        },

        goToConfig(jobCode) {
            this.navTo('config', 'list');
            this.loadJobConfigs();
        },

        goToMonitorTaskList(jobCode) {
            this.navTo('monitor', 'taskList');
            this.taskFilter.jobCode = jobCode || '';
            this.taskFilter.status = '';
            this.taskPagination.page = 0;
            this.loadTasks();
        },

        goToOpsExecute(jobCode) {
            this.navTo('ops', 'execute');
            this.selectedJobConfigKey = jobCode || '';
            this.loadConfigKeys();
        },

        applyHashJobCode() {
            const hash = window.location.hash || '';
            const q = hash.indexOf('?');
            if (q === -1) return;
            const params = new URLSearchParams(hash.slice(q));
            const jobCode = params.get('jobCode');
            if (jobCode) {
                const path = hash.slice(1, q) || '';
                if (path.startsWith('monitor/taskList')) this.goToMonitorTaskList(jobCode);
                else if (path.startsWith('config')) this.goToConfig(jobCode);
                else if (path.startsWith('ops/execute')) this.goToOpsExecute(jobCode);
            }
        },

        changePage(page) {
            if (page < 0) return;
            this.taskPagination.page = page;
            this.loadTasks();
        },

        async viewTaskDetail(taskId) {
            try {
                // 加载任务基本信息
                const taskResponse = await axios.get(`/api/tasks/${taskId}`);
                this.selectedTask = taskResponse.data;

                // 加载进度信息
                try {
                    const progressResponse = await axios.get(`/api/tasks/${taskId}/progress`);
                    this.taskProgress = progressResponse.data;
                } catch (e) {
                    this.taskProgress = null;
                }

                // 加载统计信息
                try {
                    const statsResponse = await axios.get(`/api/tasks/${taskId}/statistics`);
                    this.taskStatistics = statsResponse.data;
                } catch (e) {
                    this.taskStatistics = null;
                }

                // 显示模态框
                const modal = new bootstrap.Modal(document.getElementById('taskDetailModal'));
                modal.show();
            } catch (error) {
                console.error('加载任务详情失败:', error);
                alert('加载任务详情失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async viewTaskManifest(taskId) {
            this.taskManifest = null;
            try {
                const response = await axios.get(`/api/tasks/${taskId}/manifest`);
                this.taskManifest = response.data;
                const modal = new bootstrap.Modal(document.getElementById('manifestModal'));
                modal.show();
            } catch (error) {
                if (error.response && error.response.status === 404) {
                    this.taskManifest = null;
                    const modal = new bootstrap.Modal(document.getElementById('manifestModal'));
                    modal.show();
                } else {
                    console.error('加载 Manifest 失败:', error);
                    alert('加载 Manifest 失败: ' + (error.response?.data?.error || error.message));
                }
            }
        },

        async deleteTask(taskId) {
            if (!confirm('确定要删除这个任务吗？')) {
                return;
            }

            try {
                await axios.delete(`/api/tasks/${taskId}`);
                alert('任务删除成功');
                this.loadTasks();
                if (this.currentView === 'monitor' && this.monitorSub === 'overview') this.loadStats();
            } catch (error) {
                console.error('删除任务失败:', error);
                alert('删除任务失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async triggerJob() {
            if (!this.selectedJobConfigKey) {
                alert('请选择作业');
                return;
            }
            const key = this.selectedJobConfigKey;
            const isLoad = key.endsWith('_load');
            const url = isLoad ? `/api/jobs/${encodeURIComponent(key)}/load` : `/api/jobs/${encodeURIComponent(key)}/extract`;
            try {
                const params = isLoad && this.loadSourceBatch && this.loadSourceBatch.trim() ? { sourceBatch: this.loadSourceBatch.trim() } : {};
                const response = await axios.post(url, null, { params });
                this.jobResult = {
                    success: true,
                    message: response.data.message || (isLoad ? '加载作业已启动' : '卸载作业已启动'),
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
            } catch (error) {
                console.error('触发作业失败:', error);
                this.jobResult = {
                    success: false,
                    message: '触发失败: ' + (error.response?.data?.error || error.message)
                };
            }
        },

        async loadConfigKeys() {
            try {
                const response = await axios.get('/api/jobs/config-keys');
                this.configKeys = response.data || [];
            } catch (error) {
                console.error('加载配置 key 失败:', error);
                this.configKeys = [];
            }
        },

        async loadTableExportConfigKeys() {
            try {
                const response = await axios.get('/api/jobs/config-keys/table-export');
                this.tableExportConfigKeys = response.data || [];
            } catch (error) {
                console.error('加载表级卸载作业列表失败:', error);
                this.tableExportConfigKeys = [];
            }
        },

        async loadTableLoadConfigKeys() {
            try {
                const response = await axios.get('/api/jobs/config-keys/table-load');
                this.tableLoadConfigKeys = response.data || [];
            } catch (error) {
                console.error('加载表级加载作业列表失败:', error);
                this.tableLoadConfigKeys = [];
            }
        },

        async loadTablesForJob(configKey, type) {
            if (!configKey) {
                if (type === 'export') this.exportableTables = [];
                else this.loadableTables = [];
                return;
            }
            try {
                const response = await axios.get(`/api/jobs/${encodeURIComponent(configKey)}/tables`);
                const list = response.data || [];
                if (type === 'export') {
                    this.exportableTables = list;
                    this.tableExport.tableName = '';
                } else {
                    this.loadableTables = list;
                    this.tableLoad.tableName = '';
                }
            } catch (error) {
                console.error('加载表列表失败:', error);
                if (type === 'export') this.exportableTables = [];
                else this.loadableTables = [];
            }
        },

        async loadManualExports() {
            try {
                const response = await axios.get('/api/manual-exports', {
                    params: { page: 0, size: 50 }
                });
                this.manualExports = response.data.data || [];
            } catch (error) {
                console.error('加载表级导出历史失败:', error);
                this.manualExports = [];
            }
        },

        async loadJobConfigs() {
            try {
                const response = await axios.get('/api/jobs/configs');
                this.jobConfigList = response.data || [];
            } catch (error) {
                console.error('加载作业配置列表失败:', error);
                this.jobConfigList = [];
            }
        },

        openAddJobConfig() {
            this.jobConfigError = null;
            this.jobConfigModal = { isEdit: false, configKey: '', contentYaml: '' };
            const modal = new bootstrap.Modal(document.getElementById('jobConfigModal'));
            modal.show();
        },

        async openConfigSpec() {
            this.configSpecError = null;
            this.configSpecHtml = '';
            this.configSpecLoading = true;
            try {
                const res = await axios.get('/config-spec.md', { responseType: 'text' });
                const text = typeof res.data === 'string' ? res.data : (res.data && res.data.toString ? res.data.toString() : '');
                this.configSpecHtml = typeof marked !== 'undefined' ? marked.parse(text) : text.replace(/\n/g, '<br>');
            } catch (e) {
                this.configSpecError = e.response?.status === 404 ? '未找到配置说明文档' : (e.message || '加载失败');
            } finally {
                this.configSpecLoading = false;
            }
            this.$nextTick(() => {
                const el = document.getElementById('configSpecModal');
                if (el && window.bootstrap) new bootstrap.Modal(el).show();
            });
        },

        closeConfigSpec() {
            this.configSpecError = null;
        },

        async openEditJobConfig(configKey) {
            this.jobConfigError = null;
            this.jobConfigModal = { isEdit: true, configKey: configKey, contentYaml: '' };
            try {
                const response = await axios.get(`/api/jobs/configs/${encodeURIComponent(configKey)}`);
                this.jobConfigModal.contentYaml = response.data.contentYaml || '';
            } catch (error) {
                console.error('加载配置失败:', error);
                this.jobConfigError = '加载配置失败: ' + (error.response?.data?.error || error.message);
            }
            const modal = new bootstrap.Modal(document.getElementById('jobConfigModal'));
            modal.show();
        },

        closeJobConfigModal() {
            this.jobConfigError = null;
            this.jobConfigModal = { isEdit: false, configKey: '', contentYaml: '' };
        },

        async saveJobConfig() {
            const key = (this.jobConfigModal.configKey || '').trim();
            const yaml = (this.jobConfigModal.contentYaml || '').trim();
            if (!yaml) {
                this.jobConfigError = 'YAML 内容不能为空';
                return;
            }
            if (this.jobConfigModal.isEdit && !key) {
                this.jobConfigError = '编辑时需提供作业名';
                return;
            }
            this.jobConfigError = null;
            try {
                if (this.jobConfigModal.isEdit) {
                    await axios.put(`/api/jobs/configs/${encodeURIComponent(key)}`, { contentYaml: yaml });
                } else {
                    const body = key ? { configKey: key, contentYaml: yaml } : { contentYaml: yaml };
                    await axios.post('/api/jobs/configs', body);
                }
                bootstrap.Modal.getInstance(document.getElementById('jobConfigModal')).hide();
                this.closeJobConfigModal();
                this.loadJobConfigs();
            } catch (error) {
                this.jobConfigError = error.response?.data?.error || error.message;
            }
        },

        async deleteJobConfig(configKey) {
            if (configKey === '__global__') {
                alert('不能删除全局配置');
                return;
            }
            if (!confirm('确定要删除作业配置「' + configKey + '」吗？')) {
                return;
            }
            try {
                await axios.delete(`/api/jobs/configs/${encodeURIComponent(configKey)}`);
                this.loadJobConfigs();
            } catch (error) {
                alert('删除失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async triggerTableExport() {
            if (!this.tableExport.configKey || !this.tableExport.tableName) {
                alert('请选择卸载作业和表名');
                return;
            }
            try {
                const configKey = encodeURIComponent(this.tableExport.configKey);
                const tableName = encodeURIComponent(this.tableExport.tableName);
                const response = await axios.post(`/api/jobs/${configKey}/tables/${tableName}/export`);
                this.tableExportResult = {
                    message: response.data.message || '表级卸载已启动（按作业配置执行）',
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
                this.loadManualExports();
            } catch (error) {
                console.error('触发表级卸载失败:', error);
                alert('触发表级卸载失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async triggerTableLoad() {
            if (!this.tableLoad.configKey || !this.tableLoad.tableName) {
                alert('请选择加载作业和表名');
                return;
            }
            try {
                const configKey = encodeURIComponent(this.tableLoad.configKey);
                const tableName = encodeURIComponent(this.tableLoad.tableName);
                const params = this.tableLoad.sourceBatch && this.tableLoad.sourceBatch.trim() ? { sourceBatch: this.tableLoad.sourceBatch.trim() } : {};
                const response = await axios.post(`/api/jobs/${configKey}/tables/${tableName}/load`, null, { params });
                this.tableLoadResult = {
                    message: response.data.message || '表级加载已启动（按作业配置执行）',
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
                this.loadManualExports();
            } catch (error) {
                console.error('触发表级加载失败:', error);
                alert('触发表级加载失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async checkDiskSpace() {
            if (!this.diskPath) {
                alert('请输入路径');
                return;
            }

            try {
                const response = await axios.get('/api/system/disk-space', {
                    params: { path: this.diskPath }
                });
                this.diskInfo = response.data;
            } catch (error) {
                console.error('检查磁盘空间失败:', error);
                alert('检查磁盘空间失败: ' + (error.response?.data?.error || error.message));
            }
        },

        getStatusClass(status) {
            const classes = {
                'RUNNING': 'bg-info',
                'SUCCESS': 'bg-success',
                'FAILED': 'bg-danger',
                'CANCELLED': 'bg-secondary'
            };
            return classes[status] || 'bg-secondary';
        },

        getDiskUsageClass(percentage) {
            if (percentage >= 90) return 'bg-danger';
            if (percentage >= 75) return 'bg-warning';
            return 'bg-success';
        },

        formatDate(dateStr) {
            if (!dateStr) return '-';
            const date = new Date(dateStr);
            return date.toLocaleString('zh-CN', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        },

        formatDuration(seconds) {
            if (!seconds) return '-';
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            const secs = seconds % 60;

            if (hours > 0) {
                return `${hours}小时${minutes}分${secs}秒`;
            } else if (minutes > 0) {
                return `${minutes}分${secs}秒`;
            } else {
                return `${secs}秒`;
            }
        },

        parseExecutionLog(task) {
            if (!task || !task.executionLog) return [];
            try {
                const raw = task.executionLog;
                const arr = typeof raw === 'string' ? JSON.parse(raw) : raw;
                return Array.isArray(arr) ? arr : [];
            } catch (e) {
                return [];
            }
        },

        formatLogTime(dateStr) {
            if (!dateStr) return '-';
            const date = new Date(dateStr);
            return date.toLocaleTimeString('zh-CN', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: false
            });
        },

        isPathLogMessage(message) {
            return typeof message === 'string' && message.indexOf(' -> 目标: ') !== -1;
        }
    }
}).mount('#app');
