import time
import web_application


def benchmark_update_token(duration):
    """ 效率测试

    @param int duration: 持续时间
    @return:
    """

    for function in (web_application.update_token, web_application.update_token_pipeline):
        count = 0
        start = time.time()
        end = start + duration

        while time.time() < end:
            count += 1
            function('token', 'user', 'item')
        delta = time.time() - start
        print(function.__name__, count, delta, count / delta)


if __name__ == "__main__":
    benchmark_update_token(10)
