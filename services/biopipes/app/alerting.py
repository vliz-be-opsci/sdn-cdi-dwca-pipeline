import pymsteams

class Alerter:
    def __init__(self, webhook_url):
        # Initialise webhooks etc
        self.webhook_url = webhook_url 
        
    def get_colour(loglevel="INFO"):
        '''
        Make some colours for cards
        based on loglevel.
        '''
        if loglevel == "INFO":
           colour = "#0037ff"
        elif loglevel == "WARNING":
            colour = "#fca903"
        elif loglevel == "ERROR":
            colour = "#ff3b3b"
        else:
            colour = "#0037ff"
        return colour

    def create_msg_card(self, 
                        title = 'Message',
                        text = '', 
                        activity_title = 'Activity',
                        activity_text = '', 
                        fact_dict = {}, 
                        loglevel = "INFO"):
        '''
        Create message card based on loglevel and card_dict
        myTeamsMessage needs to be sent with "myTeamsMessage.send()"
        ''' 
        myTeamsMessage = pymsteams.connectorcard(self.webhook_url)
        colour = self.get_colour(loglevel)

        myTeamsMessage.title(title)
        myTeamsMessage.text(text)
        myTeamsMessage.color(colour)

        # create the section
        myMessageSection = pymsteams.cardsection() 

        # Activity Elements
        myMessageSection.activityTitle(activity_title) 
        myMessageSection.activityText(activity_text)

        # Section Text
        myMessageSection.text("This is my section text")

        # Facts are key value pairs displayed in a list.
        for key, value in fact_dict.items():
            myMessageSection.addFact(key, str(value))

        # Add your section to the connector card object before sending
        myTeamsMessage.addSection(myMessageSection)

        return myTeamsMessage 