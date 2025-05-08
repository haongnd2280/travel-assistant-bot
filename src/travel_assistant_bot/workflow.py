from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph, START
from langgraph.prebuilt import tools_condition

from travel_assistant_bot.state import State
from travel_assistant_bot.agent import assistant, tools
from tools.utils import create_tool_node_with_fallback
from tools.flights import fetch_user_flight_information


builder = StateGraph(State)


def fetch_user_info(state: State):
    # State passed for nothing just because this is a node.
    # The real information needed is the config that
    # is passed to the `stream` or `invoke` method
    return {"user_info": fetch_user_flight_information.invoke({})}


# NEW: The fetch_user_info node runs first, meaning our assistant can see the user's flight information without
# having to take an action
builder.add_node("fetch_user_info", fetch_user_info)
builder.add_node("assistant", assistant)
builder.add_node("tools", create_tool_node_with_fallback(tools))

builder.add_edge(START, "fetch_user_info")
builder.add_edge("fetch_user_info", "assistant")
builder.add_conditional_edges(
    "assistant",
    tools_condition,
)
builder.add_edge("tools", "assistant")

memory = MemorySaver()
graph = builder.compile(
    checkpointer=memory,
    # NEW: The graph will always halt before executing the "tools" node.
    # The user can approve or reject (or even alter the request) before
    # the assistant continues
    interrupt_before=["tools"],
)
