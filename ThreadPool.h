#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue> 
#include <thread>
#include <unordered_map>

//线程池模式
enum class ThreadPoolMode
{
	MODE_FIXED = 0,		//固定数量
	MODE_CACHED,		//动态数量
};

//线程类
class Thread
{
public:

	using ThreadFunc = std::function<void(int)>;

	Thread(ThreadFunc func)
		: func(func)
		, threadID(defaultID++)
	{
	}

	~Thread() = default;

	void run()
	{
		std::thread thread(func, threadID);
		thread.detach();			//线程分离，线程结束后自动释放资源
	}

	int getID() 
	{
		return threadID;
	}
private:
	ThreadFunc func;		//线程函数
	static int defaultID;
	int threadID;			//线程ID
};

int Thread::defaultID = 0;

//线程池类
class ThreadPool
{
public:
	ThreadPool() 
		: defaultThreadSize(0)
		, taskSize(0)
		, freeThreadSize(0)
		, currentThreadSize(0)
		, threadMaxSize(10)
		, taskQueueMaxSize(1024)
		, poolMode(ThreadPoolMode::MODE_FIXED)
		, isPoolRunning(false)
	{

	}

	~ThreadPool()
	{
		isPoolRunning = false;

		std::unique_lock<std::mutex> lock(mutex);
		notEmpty.notify_all();
		exitCond.wait(lock, [&]()->bool
			{
				return threads.size() == 0;
			});
	}

	//禁用拷贝构造函数和赋值运算符
	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

	//设置任务队列最大容量
	void setTaskQueueMaxSize(int size)
	{
		if (checkPoolState())
		{
			return;
		}
		taskQueueMaxSize = size;
	}

	//MODE_CACHED模式下线程最大数量
	void setThreadMaxSize(int size)
	{
		if (checkPoolState())
		{
			return;
		}
		if (poolMode == ThreadPoolMode::MODE_CACHED)
		{
			threadMaxSize = size;
		}
	}

	//设置线程池模式
	void setMode(ThreadPoolMode mode)
	{
		if (checkPoolState())
		{
			return;
		}
		poolMode = mode;
	}

	//启动线程池
	void start(int threadSize = std::thread::hardware_concurrency())
	{
		isPoolRunning = true;

		defaultThreadSize = threadSize;
		currentThreadSize = threadSize;
		//创建线程
		for (int i = 0; i < defaultThreadSize; i++)
		{
			//创建线程对象时绑定线程函数
			auto ptrThread = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			threads.emplace(ptrThread->getID(), std::move(ptrThread));
		}

		for (int i = 0; i < defaultThreadSize; i++)
		{
			threads[i]->run();
			freeThreadSize++;
		}
	}


	/*
	  提交任务
	  使用可变参模板编程，让submitTask可以接收任意任务函数和任意数量的参数
	  返回值future<>
	*/
	template<typename Func, typename... Args>
	auto submitTask(Func&& func, Args&&... args) -> std::future<decltype(func(args...))>
	{
		using RType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RType()>>(
			std::bind(std::forward<Func>(func),std::forward(args...)));
		
		std::future<RType> result = task->get_future();

		//获取任务队列锁
		std::unique_lock<std::mutex> lock(mutex);

		//等待任务队列非满,等待时长1s
		bool res = notFull.wait_for(lock, std::chrono::seconds(1),
			[&]()->bool
			{
				return taskQueue.size() < taskQueueMaxSize;
			});

		if (!res)
		{
			std::cerr << "taskQueue is full , submit task fail" << std::endl;
			auto task = std::make_shared<std::packaged_task<RType()>>([]()->RType { return RType(); });
			(*task)();
			return task->get_future();
		}

		//将任务加入队列
		taskQueue.emplace(task);
		taskSize++;

		//通知任务队列非空
		notEmpty.notify_all();

		if (poolMode == ThreadPoolMode::MODE_CACHED
			&& taskSize > freeThreadSize
			&& currentThreadSize < threadMaxSize)
		{
			//创建新线程
			auto ptrThread = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			auto threadID = ptrThread->getID();
			threads.emplace(threadID, std::move(ptrThread));
			threads[threadID]->run();
			currentThreadSize++;
			freeThreadSize++;

			std::cout << "create new thread" << std::endl;
		}

		return result;
	}

	bool checkPoolState()
	{
		if (isPoolRunning)
		{
			return true;
		}
		return false;
	}
private:
	void threadFunc(int threadID)

	{
		/*
		 * auto lastTime = std::chrono::high_resolution_clock().now(); 不推荐
		 * std::chrono::high_resolution_clock 是一个因标准定义宽松而导致实现混乱的组件。
		 * 它的“高分辨率”承诺往往只是对 system_clock 或 steady_clock 的简单别名，却隐藏了潜在的单调性丧失和跨平台行为不一的风险。
		 * 为了代码的清晰性、可移植性和可靠性，应直接选用语义明确的 steady_clock（用于计时）或 system_clock（用于日历时间）。
		 */
		auto lastTime = std::chrono::steady_clock::now();
		int waitTime = 10;

		//线程池析构，立即结束，不等待线程执行完成
		//while (isPoolRunning) 
		for (;;) //线程池析构，等待所有线程执行完成
		{
			Task task;
			{
				//获取锁
				std::unique_lock<std::mutex> lock(mutex);

				//锁+双重判断
				while (taskQueue.size() == 0)
				{
					if (!isPoolRunning)
					{
						if (threads.contains(threadID))
						{
							threads.erase(threadID);
							exitCond.notify_all();
						}
						return;
					}

					if (poolMode == ThreadPoolMode::MODE_CACHED)
					{
						auto res = notEmpty.wait_for(lock, std::chrono::seconds(1));
						if (res == std::cv_status::timeout)
						{
							auto now = std::chrono::steady_clock::now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
							if (dur.count() > waitTime
								&& currentThreadSize > defaultThreadSize)
							{
								if (threads.contains(threadID))
								{
									threads.erase(threadID);
									currentThreadSize--;
									freeThreadSize--;

									return;
								}
							}
						}
					}
					else
					{
						//等待任务队列非空
						notEmpty.wait(lock);
					}
				}

				freeThreadSize--;

				//获取任务
				task = taskQueue.front();
				taskQueue.pop();
				taskSize--;

				//任务队列非空，通知其他线程调度任务
				if (taskQueue.size() > 0)
				{
					notEmpty.notify_all();
				}

				//任务队列不满，通知增加任务
				notFull.notify_all();

			}//及时释放锁，避免阻塞

			//执行任务
			if (task != nullptr)
			{
				task();
			}

			freeThreadSize++;

			//更新线程执行完任务时间
			lastTime = std::chrono::steady_clock::now();
		}
	}


private:
	int defaultThreadSize;											//初始线程数量
	int taskQueueMaxSize;											//任务队列最大容量
	int threadMaxSize;												//线程最大数量
	std::atomic_int taskSize;										//任务数量
	std::atomic_int freeThreadSize;									//空闲线程数
	std::atomic_int currentThreadSize;								//当前线程池的线程数量
	std::atomic_bool isPoolRunning;									//线程池是否启动

	//std::vector<std::unique_ptr<Thread>> threads;	//线程列表
	std::unordered_map<int, std::unique_ptr<Thread>> threads;		//线程列表	
	using Task = std::function<void()>;
	std::queue<Task> taskQueue;					//任务队列

	std::mutex mutex;												//互斥锁
	std::condition_variable notEmpty;								//任务队列非空条件变量
	std::condition_variable notFull;								//任务队列非满条件变量
	std::condition_variable exitCond;								//等待线程资源全部回收

	ThreadPoolMode poolMode;										//线程池模式
};

#endif // !THREADPOOL_H