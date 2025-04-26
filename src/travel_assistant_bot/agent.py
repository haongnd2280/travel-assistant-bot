from datetime import datetime

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableConfig
from langchain_community.tools.tavily_search import TavilySearchResults

from travel_assistant_bot.state import State
from tools.cars import (
    cancel_car_rental,
    book_car_rental,
    search_car_rentals,
    update_car_rental
)
from tools.excursions import (
    cancel_excursion,
    book_excursion,
    search_trip_recommendations,
    update_excursion
)
from tools.flights import (
    cancel_ticket,
    fetch_user_flight_information,
    search_flights,
    update_ticket_to_new_flight
)
from tools.hotels import (
    book_hotel,
    cancel_hotel,
    search_hotels,
    update_hotel
)
from tools.policies import lookup_policy

from dotenv import load_dotenv
load_dotenv()


class Assistant:
    def __init__(self, runnable: Runnable):
        self.runnable = runnable

    def __call__(self, state: State, config: RunnableConfig) -> State:
        while True:
            configuration = config.get("configurable", {})
            passenger_id = configuration.get("passenger_id", None)
            # Create a new state instead of modifying the existing one
            # This is to avoid mutability issues
            state = {
                **state,
                "user_info": passenger_id
            }

            result = self.runnable.invoke(state)
            # If the LLM happens to return an empty response, we will re-prompt it
            # for an actual response.
            if (
                not result.tool_calls   # no tool calls
                and                     # no content
                (
                    not result.content
                    or
                    (
                        isinstance(result.content, list)
                        and not result.content[0].get("text")
                    )
                )
            ):
                # A shortcut format that will be converted into a proper format
                # after passing through `ChatPromptTemplate`
                messages = state["messages"] + [("user", "Respond with a real output.")]
                state = {
                    **state,
                    "messages": messages    # overwrite the messages with the new one
                }
            else:
                break

        # Return the state with the messages
        return {"messages": result}


llm = ChatOpenAI(model="gpt-4.1", temperature=0)

primary_assistant_prompt = ChatPromptTemplate.from_messages(
    [
        (
            # System message
            "system",
            "You are a helpful customer support assistant for Swiss Airlines. "
            "Use the provided tools to search for flights, company policies, and other information to assist the user's queries. "
            "When searching, be persistent. Expand your query bounds if the first search returns no results. "
            "If a search comes up empty, expand your search before giving up."
            "\n\nCurrent user:\n<User>\n{user_info}\n</User>"
            "\nCurrent time: {time}.",
        ),
        # A list of chat messages
        ("placeholder", "{messages}"),
    ]
).partial(time=datetime.now)

part_1_tools = [
    TavilySearchResults(max_results=1),
    fetch_user_flight_information,
    search_flights,
    lookup_policy,
    update_ticket_to_new_flight,
    cancel_ticket,
    search_car_rentals,
    book_car_rental,
    update_car_rental,
    cancel_car_rental,
    search_hotels,
    book_hotel,
    update_hotel,
    cancel_hotel,
    search_trip_recommendations,
    book_excursion,
    update_excursion,
    cancel_excursion,
]
# Runnable = System Message + Message List ---> LLM + Tools
runnable = primary_assistant_prompt | llm.bind_tools(part_1_tools)
assistant = Assistant(runnable)
