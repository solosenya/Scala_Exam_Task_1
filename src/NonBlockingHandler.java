import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Смысл работы такой:
 * 1. Объединяем обработку задач по обращению к сервисам, ждем выполнения хотя бы одной
 * (это гарантируется CompletableFuture.anyOf).
 * 2. Если первый из пришедших отвтеов вернул retry, то после ожидания повторяюьтся запросы до тех пор,
 * пока не будет получен результат (success/failure)
 */
public class NonBlockingHandler implements Handler {
    private final Client client = new ClientImpl();

    @Override
    public ApplicationStatusResponse performOperation(String id) {
        TaskInfo taskInfo = TaskInfo.build(id);

        CompletableFuture<Response> firstFutureResponse = callFirstStatus(taskInfo);
        CompletableFuture<Response> secondFutureResponse = callSecondStatus(taskInfo);

        try {
            return CompletableFuture.anyOf(firstFutureResponse, secondFutureResponse)
                .thenCompose((response) -> manageClientResponse(taskInfo, response))
                .handle((response, exception) -> getFailureResponse(taskInfo))
                .get();
        } catch (Exception exception) {
            return getFailureResponse(taskInfo);
        }
    }

    /**
     * Метод работает рекусривно, пока ловит retry, в противном случае возвращает результат (success/failure)
     */
    private CompletableFuture<ApplicationStatusResponse> manageClientResponse(
        TaskInfo taskInfo,
        Object response
    ) {
        if (response instanceof Response.Failure) {
            return getFailure(taskInfo);
        }

        if (response instanceof Response.Success success) {
            return getSuccess(taskInfo, success);
        }

        if (!(response instanceof Response.RetryAfter retryAfter)) {
            return getFailure(taskInfo);
        }

        try {
            Thread.sleep(retryAfter.delay());
        } catch (InterruptedException interruptedException) {
            return getFailure(taskInfo);
        }

        CompletableFuture<Response> firstFutureResponse = callFirstStatus(taskInfo);
        CompletableFuture<Response> secondFutureResponse = callSecondStatus(taskInfo);

        taskInfo.increment();

        return CompletableFuture.anyOf(firstFutureResponse, secondFutureResponse)
            .thenCompose((responseChild) -> manageClientResponse(taskInfo, response));
    }

    private CompletableFuture<Response> callFirstStatus(TaskInfo taskInfo) {
        return CompletableFuture.supplyAsync(
            () -> client.getApplicationStatus1(taskInfo.id())
        );
    }

    private CompletableFuture<Response> callSecondStatus(TaskInfo taskInfo) {
        return CompletableFuture.supplyAsync(
            () -> client.getApplicationStatus2(taskInfo.id())
        );
    }

    private ApplicationStatusResponse getFailureResponse(TaskInfo taskInfo) {
        return new ApplicationStatusResponse.Failure(getEpochMillis(), taskInfo.getRetriesCount());
    }

    private CompletableFuture<ApplicationStatusResponse> getFailure(TaskInfo taskInfo) {
        return CompletableFuture.supplyAsync(() -> getFailureResponse(taskInfo));
    }

    private ApplicationStatusResponse getSuccessResponse(TaskInfo taskInfo, Response.Success success) {
        return new ApplicationStatusResponse.Success(taskInfo.id(), success.applicationStatus());
    }

    private CompletableFuture<ApplicationStatusResponse> getSuccess(TaskInfo taskInfo, Response.Success success) {
        return CompletableFuture.supplyAsync(() -> getSuccessResponse(taskInfo, success));
    }

    private Duration getEpochMillis() {
        long millis = LocalDateTime.now()
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();
        return Duration.ofMillis(millis);
    }

    /**
     * рекорд, чтобы не тащить в методах параметры запроса
     * @param id - айди запроса
     * @param retriesCount - количество ретраев
     * @param start - время начала обработки запроса
     */
    private record TaskInfo(String id, AtomicInteger retriesCount, LocalDateTime start) {
        public static TaskInfo build(String id) {
            return new TaskInfo(id, new AtomicInteger(0), LocalDateTime.now());
        }

        public void increment() {
            retriesCount.incrementAndGet();
        }

        public int getRetriesCount() {
            return retriesCount.get();
        }
    }
}
