ui <- fluidPage(
  h1("Gapminder"),
  # Add a placeholder for a table output
  tableOutput("table")
)

server <- function(input, output) {
  # Call the appropriate render function 
  output$table <- renderTable({
    # Show the gapminder object in the table
    gapminder
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  # Add a slider for life expectancy filter 
  sliderInput(inputId = "life", label = "Life expectancy",
      min = 0, max = 120,
      value = c(30, 50)),
  tableOutput("table")
)

server <- function(input, output) {
  output$table <- renderTable({
    data <- gapminder
    data <- subset(
      data,
      # Use the life expectancy input to filter the data
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    data
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  # Add an "All" value to the continent list
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  tableOutput("table")
)

server <- function(input, output) {
  output$table <- renderTable({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    # Don't subset the data if "All" continent are chosen
    if (input$continent == "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  # Add a plot output
  plotOutput("plot"),
  tableOutput("table")
)

server <- function(input, output) {
  output$table <- renderTable({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })

  # Create the plot render function  
  output$plot <- renderPlot({
    # Use the same filtered data that the table uses
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }   
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  # Add a download button
  downloadButton(outputId = "download_data", label = "Download"),
  plotOutput("plot"),
  tableOutput("table")
)

server <- function(input, output) {
  output$table <- renderTable({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })

  # Create a download handler
  output$download_data <- downloadHandler(
    # The downloaded file is named "gapminder_data.csv"
    filename = "gapminder_data.csv",
    content = function(file) {
      # The code for filtering the data is copied from the
      # renderTable() function
      data <- gapminder
      data <- subset(
        data,
        lifeExp >= input$life[1] & lifeExp <= input$life[2]
      )
      if (input$continent != "All") {
        data <- subset(
          data,
          continent == input$continent
        )
      }
      
      # Write the filtered data into a CSV file
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  downloadButton(outputId = "download_data", label = "Download"),
  plotOutput("plot"),
  tableOutput("table")
)

server <- function(input, output) {
  # Create a reactive variable named "filtered_data"
  filtered_data <- reactive({
    # Filter the data (copied from previous exercise)
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
  
  output$table <- renderTable({
    # Use the filtered_data variable to render the table output
    data <- filtered_data()
    data
  })

  output$download_data <- downloadHandler(
    filename = "gapminder_data.csv",
    content = function(file) {
      # Use the filtered_data variable to create the data for
      # the downloaded file
      data <- filtered_data()
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    # Use the filtered_data variable to create the data for
    # the plot
    data <- filtered_data()
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  downloadButton("download_data"),
  plotOutput("plot"),
  # Replace the tableOutput() with DT's version
  DT::dataTableOutput("table")
)

server <- function(input, output) {
  filtered_data <- reactive({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
  
  # Replace the renderTable() with DT's version
  output$table <- renderDataTable({
    data <- filtered_data()
    data
  })

  output$download_data <- downloadHandler(
    filename = "gapminder_data.csv",
    content = function(file) {
      data <- filtered_data()
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    data <- filtered_data()
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
  h1("Gapminder"),
  sliderInput(inputId = "life", label = "Life expectancy",
              min = 0, max = 120,
              value = c(30, 50)),
  selectInput("continent", "Continent",
              choices = c("All", levels(gapminder$continent))),
  downloadButton("download_data"),
  plotOutput("plot"),
  # Replace the tableOutput() with DT's version
  DT::dataTableOutput("table")
)

server <- function(input, output) {
  filtered_data <- reactive({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
  
  # Replace the renderTable() with DT's version
  output$table <- DT::renderDataTable({
    data <- filtered_data()
    data
  })

  output$download_data <- downloadHandler(
    filename = "gapminder_data.csv",
    content = function(file) {
      data <- filtered_data()
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    data <- filtered_data()
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
ui <- fluidPage(
    h1("Gapminder"),
    # Create a container for tab panels
    tabsetPanel(
        # Create an "Inputs" tab
        tabPanel(
            title = "Inputs",
            sliderInput(inputId = "life", label = "Life expectancy",
                        min = 0, max = 120,
                        value = c(30, 50)),
            selectInput("continent", "Continent",
                        choices = c("All", levels(gapminder$continent))),
            downloadButton("download_data")
        ),
        # Create a "Plot" tab
        tabPanel(
            title = "Plot",
            plotOutput("plot")
        ),
        # Create "Table" tab
        tabPanel(
            title = "Table",
            DT::dataTableOutput("table")
        )
    )
)

server <- function(input, output) {
  filtered_data <- reactive({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
  
  output$table <- DT::renderDataTable({
    data <- filtered_data()
    data
  })

  output$download_data <- downloadHandler(
    filename = "gapminder_data.csv",
    content = function(file) {
      data <- filtered_data()
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    data <- filtered_data()
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
###################################################
my_css <- "
#download_data {
  /* Change the background color of the download button
     to orange. */
  background: orange;

  /* Change the text size to 20 pixels. */
  font-size: 20px;
}

#table {
  /* Change the text color of the table to red. */
  color: red;
}
"

ui <- fluidPage(
  h1("Gapminder"),
  # Add the CSS that we wrote to the Shiny app
  tags$style(my_css),
  tabsetPanel(
    tabPanel(
      title = "Inputs",
      sliderInput(inputId = "life", label = "Life expectancy",
                  min = 0, max = 120,
                  value = c(30, 50)),
      selectInput("continent", "Continent",
                  choices = c("All", levels(gapminder$continent))),
      downloadButton("download_data")
    ),
    tabPanel(
      title = "Plot",
      plotOutput("plot")
    ),
    tabPanel(
      title = "Table",
      DT::dataTableOutput("table")
    )
  )
)

server <- function(input, output) {
  filtered_data <- reactive({
    data <- gapminder
    data <- subset(
      data,
      lifeExp >= input$life[1] & lifeExp <= input$life[2]
    )
    if (input$continent != "All") {
      data <- subset(
        data,
        continent == input$continent
      )
    }
    data
  })
  
  output$table <- DT::renderDataTable({
    data <- filtered_data()
    data
  })

  output$download_data <- downloadHandler(
    filename = "gapminder_data.csv",
    content = function(file) {
      data <- filtered_data()
      write.csv(data, file, row.names = FALSE)
    }
  )

  output$plot <- renderPlot({
    data <- filtered_data()
    ggplot(data, aes(gdpPercap, lifeExp)) +
      geom_point() +
      scale_x_log10()
  })
}

shinyApp(ui, server)
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
