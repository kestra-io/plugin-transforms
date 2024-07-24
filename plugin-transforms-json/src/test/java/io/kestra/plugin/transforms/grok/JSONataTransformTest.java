package io.kestra.plugin.transforms.grok;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@KestraTest
class JSONataTransformTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGetOutputForValidExprReturningStringForFromURI() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final OutputStream os = Files.newOutputStream(ouputFilePath)) {
            FileSerde.writeAll(os, Mono.just(new ObjectMapper().readValue(TEST_DATA, Map.class)).flux()).block();
            os.flush();
        }
        URI uri = runContext.storage().putFile(ouputFilePath.toFile());

        JSONataTransform task = JSONataTransform.builder()
            .from(uri.toString())
            .expr(TEST_EXPRESSION)
            .build();

        // When
        JSONataTransform.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        String transformationResult = FileSerde.readAll(is, new TypeReference<String>() {
        }).blockLast();

        Assertions.assertEquals(TEST_EXPRESSION_RESULT, transformationResult);
    }

    @Test
    void shouldGetOutputForValidExprReturningStringForFromJSON() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        JSONataTransform task = JSONataTransform.builder()
            .from(TEST_DATA)
            .expr(TEST_EXPRESSION)
            .build();

        // When
        JSONataTransform.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        String transformationResult = FileSerde.readAll(is, new TypeReference<String>() {
        }).blockLast();

        Assertions.assertEquals(TEST_EXPRESSION_RESULT, transformationResult);
    }

    @Test
    void shouldGetOutputForValidExprReturningObjectForFromJSON() throws Exception {
        // Given
        RunContext runContext = runContextFactory.of();
        JSONataTransform task = JSONataTransform.builder()
            .from("""
                {
                  "order_id": "ABC123",
                  "customer_name": "John Doe",
                  "items": [
                    {
                      "product_id": "001",
                      "name": "Apple",
                      "quantity": 5,
                      "price_per_unit": 0.5
                    },
                    {
                      "product_id": "002",
                      "name": "Banana",
                      "quantity": 3,
                      "price_per_unit": 0.3
                    },
                    {
                      "product_id": "003",
                      "name": "Orange",
                      "quantity": 2,
                      "price_per_unit": 0.4
                    }
                  ]
                }
                """)
            .expr("""
                     {
                        "order_id": order_id,
                        "customer_name": customer_name,
                        "total_price": $sum(items.(quantity * price_per_unit))
                     }
                """)
            .build();

        // When
        JSONataTransform.Output output = task.run(runContext);

        // Then
        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.getProcessedItemsTotal());

        InputStream is = runContext.storage().getFile(output.getUri());
        JsonNode transformationResult = FileSerde.readAll(is, new TypeReference<JsonNode>() {
        }).blockLast();

        Assertions.assertEquals("{\"order_id\":\"ABC123\",\"customer_name\":\"John Doe\",\"total_price\":4.2}", transformationResult.toString());
    }

    // example from https://try.jsonata.org/
    private static final String TEST_EXPRESSION = "$sum(Account.Order.Product.(Price * Quantity))";
    private static final String TEST_EXPRESSION_RESULT = "90.57000000000001";

    private static final String TEST_DATA = """
            {
          "Account": {
            "Account Name": "Firefly",
            "Order": [
              {
                "OrderID": "order103",
                "Product": [
                  {
                    "Product Name": "Bowler Hat",
                    "ProductID": 858383,
                    "SKU": "0406654608",
                    "Description": {
                      "Colour": "Purple",
                      "Width": 300,
                      "Height": 200,
                      "Depth": 210,
                      "Weight": 0.75
                    },
                    "Price": 34.45,
                    "Quantity": 2
                  },
                  {
                    "Product Name": "Trilby hat",
                    "ProductID": 858236,
                    "SKU": "0406634348",
                    "Description": {
                      "Colour": "Orange",
                      "Width": 300,
                      "Height": 200,
                      "Depth": 210,
                      "Weight": 0.6
                    },
                    "Price": 21.67,
                    "Quantity": 1
                  }
                ]
              }
            ]
          }
        }
        """;
}