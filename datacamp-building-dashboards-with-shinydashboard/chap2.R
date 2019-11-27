#######################################################################
sidebar <- dashboardSidebar(
  # Add a slider
  sliderInput(inputId="height", label="Height", min=66, max=264, value=264)
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = dashboardBody()
                    )
shinyApp(ui, server) 
#######################################################################
library(shiny)
sidebar <- dashboardSidebar(
  # Create a select list
  selectInput(inputId="name", label="Name", choices=starwars$name)
)

body <- dashboardBody(
  textOutput(outputId="name")
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = body
                    )

server <- function(input, output) {
  output$name <- renderText({
      input$name
    })
}

shinyApp(ui, server)
#######################################################################
library("shiny")

server <- function(input, output, session) {
  reactive_starwars_data <- reactiveFileReader(
         intervalMillis=1000,
         session=session,
         filePath=starwars_url,
         readFunc = function(filePath) { 
           read.csv(url(filePath))
         }
  )
}

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
shinyApp(ui, server)
#######################################################################
library(shiny)

server <- function(input, output, session) {
  reactive_starwars_data <- reactiveFileReader(
        intervalMillis = 1000,
        session = session,
        filePath = starwars_url,
        readFunc = function(filePath) { 
           read.csv(url(filePath))
         }
         )
    
  output$table <- renderTable({
    reactive_starwars_data()
  })
}

body <- dashboardBody(
  tableOutput("table")
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = dashboardSidebar(),
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
starwars <- read.csv(starwars_filepath)

server <- function(input, output) { 
   }
#######################################################################
server <- function(input, output) {
  output$task_menu <- renderMenu({
      tasks <- apply(task_data, 1, function(row) {
          taskItem(text=row[["text"]], value=row[["value"]])
        }
      )
      dropdownMenu(type="tasks", .list=tasks)
  })
}

header <- dashboardHeader({ dropdownMenuOutput("task_menu") })

ui <- dashboardPage(header = header,
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
shinyApp(ui, server)
#######################################################################
library("shiny")
sidebar <- dashboardSidebar(
  actionButton("click", "Update click box")
) 

server <- function(input, output) {
  output$click_box <- renderValueBox({
    valueBox(
      value=input$click,
      subtitle="Click box"
    )
  })
}

body <- dashboardBody(
      valueBoxOutput("click_box")
 )


ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
