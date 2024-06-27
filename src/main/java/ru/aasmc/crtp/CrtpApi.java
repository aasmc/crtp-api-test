package ru.aasmc.crtp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CrtpApi {
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String APPLICATION_JSON_VALUE = "application/json";
    private static final String SIGNATURE_HEADER = "X-Signature";
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private final JsonSerializer jsonSerializer;
    private final RequestSender requestSender;
    private final RateLimiter rateLimiter;

    public CrtpApi(TimeUnit timeUnit, int requestLimit) {
        jsonSerializer = new JacksonJsonSerializer();
        requestSender = new HttpClientJsonRequestSender();
        rateLimiter = new SimpleTokenBucketRateLimiter(timeUnit, requestLimit);
    }

    public HttpResponse<String> createDocument(CreateDocumentRequest request, String signature) {
        rateLimiter.acquireOrWait();
        String docStr = jsonSerializer.serialize(request);
        return requestSender.post(docStr, API_URL, Map.of(
                CONTENT_TYPE_HEADER, APPLICATION_JSON_VALUE,
                SIGNATURE_HEADER, signature
        ));
    }

    public interface RateLimiter {

        void acquireOrWait();

    }

    /**
     * Реализует рейт-лимитинг с помощью простого алгоритма Token Bucket, когда выделяется
     * корзина с определенным количеством доступных токенов (capacity), и приходящие запросы
     * забирают токены, если они есть. Если токенов нет, то запросы встают в очередь ожидания
     * на Condition и ждут пополнения корзины, которое происходит 1 раз в указанный интервал
     * времени. При этом корзина наполняется полностью. Такой подход гарантирует, что в
     * указанный интервал времени будет произведено не больше указанного количества запросов.
     *
     * Пополнение корзины осуществляется с помощью ScheduledExecutorService с одним потоком. В ThreadFactory
     * возвращаем демон-поток, чтобы программа успешно завершилась без закрытия ScheduledExecutorService.
     */
    public class SimpleTokenBucketRateLimiter implements RateLimiter {
        private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, r -> {
            var t = new Thread(r);
            t.setDaemon(true);
            t.setName("TokenBucketRateLimiterThread");
            return t;
        });

        private final Lock lock = new ReentrantLock();
        private final Condition tokensAreAvailable = lock.newCondition();
        private final int capacity;
        private int availableTokens;

        public SimpleTokenBucketRateLimiter(TimeUnit timeUnit, int capacity) {
            this.capacity = capacity;
            this.availableTokens = capacity;
            long refillIntervalMillis = timeUnit.toMillis(1);
            executor.scheduleAtFixedRate(this::refillTokens, refillIntervalMillis, refillIntervalMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void acquireOrWait() {
            lock.lock();
            try {
                while (availableTokens == 0) {
                    waitForRefill();
                }
                availableTokens--;
            } finally {
                lock.unlock();
            }
        }

        private void waitForRefill() {
            try {
                tokensAreAvailable.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void refillTokens() {
            lock.lock();
            try {
                availableTokens = capacity;
                tokensAreAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public interface RequestSender {

        HttpResponse<String> post(String body, String url, Map<String, String> headers);

    }

    public class HttpClientJsonRequestSender implements RequestSender {

        private final HttpClient client = HttpClient.newHttpClient();

        @Override
        public HttpResponse<String> post(String body, String url, Map<String, String> headers) {
            var builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .POST(HttpRequest.BodyPublishers.ofString(body));

            headers.forEach(builder::header);

            var request = builder.build();

            try {
                return client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public interface JsonSerializer {
        <T> String serialize(T object);

    }

    public class JacksonJsonSerializer implements JsonSerializer {

        /**
         * Экземпляр ObjectMapper является потокобезопасным, если его конфигурация осуществляется
         * до вызова методов чтения или записи. Статическая переменная гарантирует это поведение.
         */
        private static final ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        @Override
        public <T> String serialize(T object) {
            try {
                return objectMapper.writeValueAsString(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public record CreateDocumentRequest(
            DocumentDescription description,
            @JsonProperty("doc_id")
            String docId,
            @JsonProperty("doc_status")
            String docStatus,
            @JsonProperty("doc_type")
            DocType docType,
            Boolean importRequest,
            @JsonProperty("owner_inn")
            String ownerInn,
            @JsonProperty("participant_inn")
            String participantInn,
            @JsonProperty("producer_inn")
            String producerInn,
            @JsonProperty("production_date")
            LocalDate productionDate,
            @JsonProperty("production_type")
            String productionType,
            List<Product> products,
            @JsonProperty("reg_date")
            LocalDate regDate,
            @JsonProperty("reg_number")
            String regNumber
    ) {
    }

    public record DocumentDescription(
            String participantInn
    ) {
    }

    public record Product(
            @JsonProperty("certificate_document")
            String certificateDocument,
            @JsonProperty("certificate_document_date")
            LocalDate certificateDocumentDate,
            @JsonProperty("certificate_document_number")
            String certificatedDocumentNumber,
            @JsonProperty("owner_inn")
            String ownerInn,
            @JsonProperty("producer_inn")
            String producerInn,
            @JsonProperty("production_date")
            LocalDate productionDate,
            @JsonProperty("tnved_code")
            String tnvedCode,
            @JsonProperty("uit_code")
            String uitCode,
            @JsonProperty("uitu_code")
            String uituCode
    ) {
    }

    public enum DocType {
        LP_INTRODUCE_GOODS
    }
}

