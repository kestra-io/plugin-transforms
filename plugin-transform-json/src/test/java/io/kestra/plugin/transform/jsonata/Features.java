package io.kestra.plugin.transform.jsonata;

public final class Features {

    // example from https://try.jsonata.org/
    public static final String DATASET_ACCOUNT_ORDER_EXPR = "$sum(Account.Order.Product.(Price * Quantity))";
    public static final String DATASET_ACCOUNT_ORDER_EXPR_RESULT = "90.57000000000001";
    public static final String DATASET_ACCOUNT_ORDER_JSON = """
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
