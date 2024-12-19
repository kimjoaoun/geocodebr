parent_function <- function() message_test()

message_test <- function() geocodebr_message(c("*" = "info"))

test_that("message_works_correctly", {
  expect_message(parent_function(), class = "geocodebr_message_test")
  expect_message(parent_function(), class = "geocodebr_message")

  expect_snapshot(parent_function(), cnd_class = TRUE)
})
