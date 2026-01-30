const { createApp } = Vue;

createApp({
    data() {
        return {
            currentView: 'dashboard',
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
            extractJobCode: '',
            loadJobCode: '',
            jobResult: null,
            diskPath: '/data',
            diskInfo: null,
            autoRefresh: null,
            configKeys: [],
            exportableTables: [],
            tableExport: { configKey: '', tableName: '', mode: 'full' },
            tableExportResult: null,
            manualExports: [],
            jobConfigList: [],
            jobConfigModal: {
                isEdit: false,
                configKey: '',
                contentYaml: ''
            },
            jobConfigError: null
        };
    },
    mounted() {
        this.loadStats();
        this.loadRecentTasks();
        this.loadTasks();

        // 自动刷新（每5秒）
        this.autoRefresh = setInterval(() => {
            if (this.currentView === 'dashboard') {
                this.loadStats();
                this.loadRecentTasks();
            } else if (this.currentView === 'tasks') {
                this.loadTasks();
            } else if (this.currentView === 'tableExport') {
                this.loadManualExports();
            } else if (this.currentView === 'jobConfig') {
                this.loadJobConfigs();
            }
        }, 5000);
    },
    beforeUnmount() {
        if (this.autoRefresh) {
            clearInterval(this.autoRefresh);
        }
    },
    methods: {
        async loadStats() {
            try {
                const response = await axios.get('/api/tasks/stats');
                this.stats = response.data;
            } catch (error) {
                console.error('加载统计信息失败:', error);
            }
        },

        async loadRecentTasks() {
            try {
                const response = await axios.get('/api/tasks', {
                    params: { page: 0, size: 10 }
                });
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

                if (this.taskFilter.jobCode) {
                    params.jobCode = this.taskFilter.jobCode;
                }
                if (this.taskFilter.status) {
                    params.status = this.taskFilter.status;
                }

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

        async deleteTask(taskId) {
            if (!confirm('确定要删除这个任务吗？')) {
                return;
            }

            try {
                await axios.delete(`/api/tasks/${taskId}`);
                alert('任务删除成功');
                this.loadTasks();
                this.loadStats();
            } catch (error) {
                console.error('删除任务失败:', error);
                alert('删除任务失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async triggerExtract() {
            if (!this.extractJobCode) {
                alert('请输入作业编码');
                return;
            }

            try {
                const response = await axios.post(`/api/jobs/${this.extractJobCode}/extract`);
                this.jobResult = {
                    success: true,
                    message: response.data.message || '卸载作业已启动',
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
                this.extractJobCode = '';
            } catch (error) {
                console.error('触发卸载作业失败:', error);
                this.jobResult = {
                    success: false,
                    message: '触发失败: ' + (error.response?.data?.error || error.message)
                };
            }
        },

        async triggerLoad() {
            if (!this.loadJobCode) {
                alert('请输入作业编码');
                return;
            }

            try {
                const response = await axios.post(`/api/jobs/${this.loadJobCode}/load`);
                this.jobResult = {
                    success: true,
                    message: response.data.message || '加载作业已启动',
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
                this.loadJobCode = '';
            } catch (error) {
                console.error('触发加载作业失败:', error);
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

        async loadExportableTables() {
            if (!this.tableExport.configKey) {
                this.exportableTables = [];
                return;
            }
            try {
                const response = await axios.get(`/api/jobs/${encodeURIComponent(this.tableExport.configKey)}/tables`);
                this.exportableTables = response.data || [];
                this.tableExport.tableName = '';
            } catch (error) {
                console.error('加载可导出表失败:', error);
                this.exportableTables = [];
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
            const yaml = this.jobConfigModal.contentYaml || '';
            if (!key) {
                this.jobConfigError = '配置 Key 不能为空';
                return;
            }
            if (!yaml) {
                this.jobConfigError = 'YAML 内容不能为空';
                return;
            }
            this.jobConfigError = null;
            try {
                if (this.jobConfigModal.isEdit) {
                    await axios.put(`/api/jobs/configs/${encodeURIComponent(key)}`, { contentYaml: yaml });
                } else {
                    await axios.post('/api/jobs/configs', { configKey: key, contentYaml: yaml });
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

        async importConfigToDb() {
            try {
                const response = await axios.post('/api/jobs/admin/import-config');
                alert(response.data.message || '导入完成');
            } catch (error) {
                console.error('导入配置失败:', error);
                alert('导入失败: ' + (error.response?.data?.error || error.message));
            }
        },

        async triggerTableExport() {
            if (!this.tableExport.configKey || !this.tableExport.tableName) {
                alert('请选择作业配置和表名');
                return;
            }
            try {
                const configKey = encodeURIComponent(this.tableExport.configKey);
                const tableName = encodeURIComponent(this.tableExport.tableName);
                const response = await axios.post(
                    `/api/jobs/${configKey}/tables/${tableName}/export?mode=${this.tableExport.mode}`
                );
                this.tableExportResult = {
                    message: response.data.message || '表级导出已启动',
                    taskId: response.data.taskId,
                    batchNumber: response.data.batchNumber
                };
                this.loadManualExports();
            } catch (error) {
                console.error('触发表级导出失败:', error);
                alert('触发表级导出失败: ' + (error.response?.data?.error || error.message));
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
        }
    }
}).mount('#app');
