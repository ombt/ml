#######################################################################
library(shinydashboard)

# Create an empty header
header <-dashboardHeader()

# Create an empty sidebar
sidebar <- dashboardSidebar()

# Create an empty body
body <- dashboardBody()
#######################################################################
header <- dashboardHeader()
sidebar <- dashboardSidebar()
body <- dashboardBody()

# Create the UI using the header, sidebar, and body
ui <- dashboardPage(header, sidebar, body)

server <- function(input, output) {}

shinyApp(ui, server)
#######################################################################
header <- dashboardHeader(
  dropdownMenu(
    type = "messages",
    messageItem(
        from = "Lucy",
        message = "You can view the International Space Station!",
        href = "https://spotthestation.nasa.gov/sightings/"
        ),
    # Add a second messageItem() 
    messageItem(
        from = "Lucy",
        message = "Learn more about the International Space Station",
        href = "https://spotthestation.nasa.gov/faq.cfm"
        )

    )
)

ui <- dashboardPage(header = header,
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
shinyApp(ui, server)
#######################################################################
header <- dashboardHeader(
  # Create a notification drop down menu
  dropdownMenu(
    type = "notifications",
    notificationItem(text="The International Space Station is overheadf")
  )
)

# Use the new header
ui <- dashboardPage(header = header,
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
                    
shinyApp(ui, server)
#######################################################################
header <- dashboardHeader(
  # Create a notification drop down menu
  dropdownMenu(
    type = "notifications",
    notificationItem(
    text = "The International Space Station is overhead"
    )
  )
)

# Use the new header
ui <- dashboardPage(header = header,
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
                    
shinyApp(ui, server)
#######################################################################
header <- dashboardHeader(
  # Create a tasks drop down menu
  dropdownMenu(
    type = "tasks",
    taskItem(text = "Mission Learn Shiny Dashboard", value = 10)
    )
)

# Use the new header
ui <- dashboardPage(header = header,
                    sidebar = dashboardSidebar(),
                    body = dashboardBody()
                    )
shinyApp(ui, server)
#######################################################################
sidebar <- dashboardSidebar(
  sidebarMenu(
  # Create two `menuItem()`s, "Dashboard" and "Inputs"
    menuItem("Dashboard", tabName="dashboard"),
    menuItem("Inputs", tabName="inputs")
  )
)

# Use the new sidebar
ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = dashboardBody()
                    )
shinyApp(ui, server)
#######################################################################
body <- dashboardBody(
  tabItems(
  # Add two tab items, one with tabName "dashboard" and one with tabName "inputs"
    tabItem(tabName = "dashboard"),
    tabItem(tabName = "inputs")
  )
)

# Use the new body
ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
library("shiny")
body <- dashboardBody(
  # Create a tabBox
  tabItems(
    tabItem(
      tabName = "dashboard",
      tabBox(
        title="International Space Station fun Facts",
        tabPanel("Fun Fact 1", "Boo 1"),
        tabPanel("Fun Fact 2", "Boo 2")
      )
    ),
    tabItem(tabName = "inputs")
  )
)

ui <- dashboardPage(header = dashboardHeader(),
                    sidebar = sidebar,
                    body = body
                    )
shinyApp(ui, server)
#######################################################################
