#######################################################################
library("shiny")

body <- dashboardBody(
  h1("Hello World!"),
# Row 1
  fluidRow(
    box(
      width=12,
      title="Regular Box, Row 1",
      "Star Wars"
    )
  ),
# Row 2
  fluidRow(
    box(
      width=12,
      title="Regular Box, Row 2",
      "Nothing but Star Wars"
    )
  )
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = dashboardSidebar(),
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
library("shiny")

body <- dashboardBody(
  h1("Hello World!"),
# Row 1
  fluidRow(
    box(
      width=12,
      title="Regular Box, Row 1",
      "Star Wars"
    ),
# Row 2
    box(
      width=12,
      title="Regular Box, Row 2",
      "Nothing but Star Wars"
    )
  )
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = dashboardSidebar(),
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
library("shiny")

body <- dashboardBody(
  fluidRow(
# Column 1
    column(
      width=6,
      infoBox(
        width = NULL,
        title = "Regular Box, Column 1",
        subtitle = "Gimme those Star Wars"
      )
    ),
# Column 2
    column(
      width=6,
      infoBox(
        width = NULL,
        title = "Regular Box, Column 2",
        subtitle = "Don't let them end"
      )
    )
  )
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = dashboardSidebar(),
                    body = body
                    )
shinyApp(ui, server)
#######################################################################

#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################ll

