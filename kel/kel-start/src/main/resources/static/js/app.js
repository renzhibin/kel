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
            autoRefresh: null
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
