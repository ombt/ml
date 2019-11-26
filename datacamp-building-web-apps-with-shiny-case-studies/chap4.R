###################################################
# Define UI for the application
ui <- fluidPage(
  h1("Word Cloud"),
  # Add the word cloud output placeholder to the UI
  wordcloud2Output(outputId = "cloud")
)

# Define the server logic
server <- function(input, output) {
  # Render the word cloud and assign it to the output list
  output$cloud <- renderWordcloud2({
    # Create a word cloud object
    create_wordcloud(artofwar)
  })
}

# Run the application
shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  # Add a numeric input for the number of words
  numericInput(inputId = "num", label = "Maximum number of words",
      value = 100, min = 5),
  # Add a color input for the background color
  colourInput("col","Background color", value="white"),
  wordcloud2Output("cloud")
)

server <- function(input, output) {
  output$cloud <- renderWordcloud2({
    # Use the values from the two inputs as
    # parameters to the word cloud
    create_wordcloud(artofwar,
                     num_words = input$num, background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  # Add a sidebar layout to the UI
  sidebarLayout(
    # Define a sidebar panel around the inputs
    sidebarPanel(
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    # Define a main panel around the output
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  output$cloud <- renderWordcloud2({
    create_wordcloud(artofwar,
                     num_words = input$num, background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      # Add a textarea input
      textAreaInput("text","Enter text",rows=7),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  output$cloud <- renderWordcloud2({
    # Use the textarea's value as the word cloud data source
    create_wordcloud(data = input$text, num_words = input$num,
                     background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      textAreaInput("text", "Enter text", rows = 7),
      # Add a file input
      fileInput("file","Select a file"),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  output$cloud <- renderWordcloud2({
    create_wordcloud(input$text, num_words = input$num,
                     background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      textAreaInput("text", "Enter text", rows = 7),
      fileInput("file", "Select a file"),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  # Define a reactive variable named `input_file`
  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    # Read the text in the uploaded file
    readLines(input$file$datapath)
  })

  output$cloud <- renderWordcloud2({
    # Use the reactive variable as the word cloud data source
    create_wordcloud(data = input_file(), num_words = input$num,
                     background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      # Add radio buttons input
      radioButtons(
        inputId = "source",
        label = "Word source",
        choices = c(
          # First choice is "book", with "Art of War" displaying
          "Art of War" = "book",
          # Second choice is "own", with "Use your own words" displaying
          "Use your own words" = "own",
          # Third choice is "file", with "Upload a file" displaying
          "Upload a file" = "file"
        )
      ),
      textAreaInput("text", "Enter text", rows = 7),
      fileInput("file", "Select a file"),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    readLines(input$file$datapath)
  })

  output$cloud <- renderWordcloud2({
    create_wordcloud(input_file(), num_words = input$num,
                     background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      radioButtons(
        inputId = "source",
        label = "Word source",
        choices = c(
          "Art of War" = "book",
          "Use your own words" = "own",
          "Upload a file" = "file"
        )
      ),
      textAreaInput("text", "Enter text", rows = 7),
      fileInput("file", "Select a file"),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  # Create a "data_source" reactive variable
  data_source <- reactive({
    # Return the appropriate data source depending on
    # the chosen radio button
    if (input$source == "book") {
      data <- artofwar
    } else if (input$source == "own") {
      data <- input$text
    } else if (input$source == "file") {
      data <- input_file()
    }
    return(data)
  })

  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    readLines(input$file$datapath)
  })

  output$cloud <- renderWordcloud2({
    # Use the data_source reactive variable as the data
    # in the word cloud function
    create_wordcloud(data = data_source(), num_words = input$num,
                     background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      radioButtons(
        inputId = "source",
        label = "Word source",
        choices = c(
          "Art of War" = "book",
          "Use your own words" = "own",
          "Upload a file" = "file"
        )
      ),
      conditionalPanel(
        condition = "input.source == 'own'",
        textAreaInput("text", "Enter text", rows = 7)
      ),
      # Wrap the file input in a conditional panel
      conditionalPanel(
        # The condition should be that the user selects
        # "file" from the radio buttons
        condition = "input.source == 'file'",
        fileInput("file", "Select a file")
      ),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  data_source <- reactive({
    if (input$source == "book") {
      data <- artofwar
    } else if (input$source == "own") {
      data <- input$text
    } else if (input$source == "file") {
      data <- input_file()
    }
    return(data)
  })
  
  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    readLines(input$file$datapath)
  })
  
  output$cloud <- renderWordcloud2({
    create_wordcloud(data_source(), num_words = input$num,
                        background = input$col)
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      radioButtons(
        inputId = "source",
        label = "Word source",
        choices = c(
          "Art of War" = "book",
          "Use your own words" = "own",
          "Upload a file" = "file"
        )
      ),
      conditionalPanel(
        condition = "input.source == 'own'",
        textAreaInput("text", "Enter text", rows = 7)
      ),
      conditionalPanel(
        condition = "input.source == 'file'",
        fileInput("file", "Select a file")
      ),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  data_source <- reactive({
    if (input$source == "book") {
      data <- artofwar
    } else if (input$source == "own") {
      data <- input$text
    } else if (input$source == "file") {
      data <- input_file()
    }
    return(data)
  })
  
  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    readLines(input$file$datapath)
  })
  
  output$cloud <- renderWordcloud2({
    # Isolate the code to render the word cloud so that it will
    # not automatically re-render on every parameter change
    isolate({
      # Render the word cloud using inputs and reactives
      create_wordcloud(data = data_source(), num_words = input$num,
                       background = input$col)
    })
  })
}

shinyApp(ui = ui, server = server)
###################################################
ui <- fluidPage(
  h1("Word Cloud"),
  sidebarLayout(
    sidebarPanel(
      radioButtons(
        inputId = "source",
        label = "Word source",
        choices = c(
          "Art of War" = "book",
          "Use your own words" = "own",
          "Upload a file" = "file"
        )
      ),
      conditionalPanel(
        condition = "input.source == 'own'",
        textAreaInput("text", "Enter text", rows = 7)
      ),
      conditionalPanel(
        condition = "input.source == 'file'",
        fileInput("file", "Select a file")
      ),
      numericInput("num", "Maximum number of words",
                   value = 100, min = 5),
      colourInput("col", "Background color", value = "white"),
      # Add a "draw" button to the app
      actionButton(inputId = "draw", label = "Draw!")
    ),
    mainPanel(
      wordcloud2Output("cloud")
    )
  )
)

server <- function(input, output) {
  data_source <- reactive({
    if (input$source == "book") {
      data <- artofwar
    } else if (input$source == "own") {
      data <- input$text
    } else if (input$source == "file") {
      data <- input_file()
    }
    return(data)
  })
  
  input_file <- reactive({
    if (is.null(input$file)) {
      return("")
    }
    readLines(input$file$datapath)
  })
  
  output$cloud <- renderWordcloud2({
    # Add the draw button as a dependency to
    # cause the word cloud to re-render on click
    input$draw
    isolate({
      create_wordcloud(data_source(), num_words = input$num,
                       background = input$col)
    })
  })
}

shinyApp(ui = ui, server = server)
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
###################################################
