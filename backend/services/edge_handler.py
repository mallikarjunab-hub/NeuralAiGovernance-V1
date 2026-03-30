"""
Edge Case Handler — catches non-analytical queries BEFORE hitting Gemini API.
Zero cost. Instant response. Professional tone.
"""
import re, logging

logger = logging.getLogger(__name__)

# ── Pattern banks ─────────────────────────────────────────────

_GREETINGS = [
    r"^(hi|hello|hey|hii+|helo|namaste|namaskar|vanakkam|good\s*(morning|afternoon|evening|day|night))[\s!.?]*$",
    r"^(howdy|greetings|sup|whats?\s*up|yo|hola|bonjour|salaam)[\s!.?]*$",
    r"^(good\s*morning|good\s*afternoon|good\s*evening|good\s*night)[\s!.?]*$",
]

_IDENTITY = [
    r"(who|what)\s+(are|r)\s+(you|u)",
    r"(your|ur)\s+name",
    r"are\s+you\s+(ai|bot|human|real|chatbot|robot|machine|gpt|claude|gemini)",
    r"(tell|about)\s+(me\s+)?about\s+(yourself|you)",
    r"what\s+can\s+you\s+do",
    r"what\s+do\s+you\s+do",
    r"(introduce)\s+(yourself)",
    r"which\s+(ai|model|llm|technology)\s+(are|is|do)\s+you",
    r"(powered|built|made|developed|created)\s+by",
    r"are\s+you\s+better\s+than",
]

_THANKS = [
    r"^(thanks?|thank\s*you|ty|thx|thanku|dhanyavaad|dhanyawad|shukriya)[\s!.?]*$",
    r"^(that.?s?\s+(great|helpful|perfect|awesome|nice|good|excellent|wonderful))[\s!.?]*$",
    r"^(great|perfect|awesome|excellent|wonderful|brilliant|fantastic)[\s!.?]*$",
]

_GOODBYE = [
    r"^(bye|goodbye|good\s*bye|see\s*you|tata|alvida|cya|take\s*care|later)[\s!.?]*$",
    r"^(have\s+a\s+(good|great|nice|wonderful)\s+(day|evening|night))[\s!.?]*$",
]

_SILLY = [
    # Jokes and entertainment
    r"(tell|say)\s+(me\s+)?(a\s+)?joke",
    r"(tell|say)\s+(me\s+)?(a\s+)?funny",
    r"make\s+me\s+laugh",
    r"sing\s+(a\s+)?song",
    r"write\s+(a\s+)?(poem|story|essay|rap|song|lyrics)",
    r"(play|let.*play)\s+(a\s+)?game",

    # Personal feelings
    r"do\s+you\s+(like|love|hate|feel|think|dream|sleep|eat|drink|breathe)",
    r"(favorite|favourite)\s+(color|colour|food|movie|song|book|sport|animal)",
    r"how\s+old\s+are\s+you",
    r"where\s+do\s+you\s+(live|stay|come\s+from)",
    r"are\s+you\s+(happy|sad|angry|tired|bored|excited|hungry|scared)",
    r"(marry|date|love|kiss|hug)\s+me",
    r"will\s+you\s+(marry|date|be\s+my)",
    r"do\s+you\s+have\s+(feelings|emotions|heart|soul|family|friends)",
    r"can\s+you\s+(feel|dream|cry|laugh|love|dance)",

    # Philosophical / random
    r"what\s+is\s+the\s+(meaning|purpose)\s+of\s+(life|everything)",
    r"(is\s+god|does\s+god)\s+(real|exist)",
    r"what\s+(happens|comes)\s+after\s+death",
    r"who\s+created\s+the\s+(world|universe)",

    # Comparisons with other AI
    r"(better|worse)\s+than\s+(chatgpt|gpt|openai|claude|gemini|copilot)",
    r"vs\s+(chatgpt|gpt|claude|gemini|copilot|bard)",

    # Random tasks
    r"translate\s+(this|to|into)\s+",
    r"(write|draft|compose)\s+(a\s+)?(email|letter|message|whatsapp|cv|resume)",
    r"solve\s+(this\s+)?(math|equation|problem|puzzle)",
    r"what\s+is\s+\d+\s*[\+\-\*\/]\s*\d+",  # math calculations
    r"(predict|forecast)\s+(future|stock|crypto|price|weather)",
    r"give\s+me\s+(advice|tips)\s+on\s+(life|love|money|career|health)",

    # Insults / rude
    r"(stupid|dumb|useless|idiot|fool|trash|garbage)\s*(bot|ai|system|app)?",
    r"you\s+(suck|are\s+bad|are\s+useless|are\s+dumb)",
]

_CONFUSED = [
    r"^(i\s+don.?t\s+(know|understand)|what|huh|what\?|confused|i\s*m\s+confused)[\s!.?]*$",
    r"^(help|help\s*me|i\s+need\s+help)[\s!.?]*$",
    r"^(hmm+|umm+|ok+|okay|k|yes|no|yeah|nah|sure|right|got\s*it)[\s!.?]*$",
    r"^(start|begin|let.s\s*(start|begin|go))[\s!.?]*$",
    r"^(what\s+next|now\s+what|then\s+what|so\s+what)[\s!.?]*$",
    r"^\?+$",  # just question marks
]

_OFF_TOPIC = [
    r"(weather|cricket|football|soccer|movie|film|song|recipe|news|horoscope)\b",
    r"(stock|share|market|bitcoin|crypto|nft|investment)\b",
    r"(train|flight|bus|ticket|booking|hotel|travel|visa)\b",
    r"(exam|result|admission|school|college|university|job|interview)\b",
    r"(cooking|food|restaurant|diet|exercise|workout|gym)\b",
    r"(politics|election|party|minister|parliament)\b",
    r"(amazon|flipkart|shopping|buy|sell|price\s+of)\b",
]

_PROFANITY_REDIRECT = [
    r"\b(fuck|shit|damn|bastard|hell|crap|ass)\b",
]

# ── Response templates ────────────────────────────────────────

_SUGGESTION_BLOCK = (
    "\n\nYou can ask me things like:\n"
    "  \"How many active beneficiaries are in Bardez taluka?\"\n"
    "  \"Compare North Goa vs South Goa beneficiaries\"\n"
    "  \"What documents are required to apply for DSSY?\"\n"
    "  \"Show category-wise monthly payout\""
)

_RESPONSES = {
    "greeting": (
        "Namaste! I'm the DSSY Statistical Analysis Assistant for the "
        "Department of Social Welfare, Government of Goa. I can help you with:\n\n"
        "Beneficiary statistics — district, taluka, category-wise counts\n"
        "Scheme information — eligibility, financial assistance, application process\n"
        "Analytical queries — trends, comparisons, payment compliance\n\n"
        "How may I assist you today?"
    ),
    "identity": (
        "I am the **Neural AI Governance** system — a statistical analysis assistant "
        "built specifically for the Dayanand Social Security Scheme (DSSY), "
        "Department of Social Welfare, Government of Goa.\n\n"
        "My capabilities:\n"
        "  Real-time DSSY beneficiary database queries\n"
        "  District, taluka, village, and category-wise analytics\n"
        "  Scheme eligibility, documents, and financial assistance information\n"
        "  Payment compliance and trend analysis\n\n"
        "I am not a general-purpose chatbot — I'm purpose-built for DSSY governance. "
        "How may I help you?"
    ),
    "thanks": (
        "You're welcome! Feel free to ask any other questions about DSSY "
        "beneficiaries, scheme eligibility, or statistics."
    ),
    "goodbye": (
        "Thank you for using the DSSY AI Query System. Have a great day! "
        "You can return anytime for beneficiary statistics or scheme information."
    ),
    "silly": (
        "I appreciate the question! However, I'm a **purpose-built analytics system** "
        "for the Dayanand Social Security Scheme (DSSY) — not a general-purpose assistant.\n\n"
        "I'm not able to help with that, but I'm very good at DSSY-related queries."
    ),
    "profanity": (
        "I understand you may be frustrated. I'm here to help with DSSY scheme "
        "queries and beneficiary data. Let's keep it professional and I'll do "
        "my best to assist you."
    ),
    "confused": (
        "No worries! Here are some things you can ask me:\n\n"
        "**Data Queries:**\n"
        "  \"How many total beneficiaries are there?\"\n"
        "  \"Show taluka-wise active beneficiary count\"\n"
        "  \"Compare North Goa vs South Goa beneficiaries\"\n"
        "  \"What is the gender-wise breakdown?\"\n"
        "  \"Which category has the highest payout?\"\n\n"
        "**Scheme Information:**\n"
        "  \"What are the eligibility criteria for DSSY?\"\n"
        "  \"How much pension do senior citizens receive?\"\n"
        "  \"What documents are needed to apply?\"\n"
        "  \"What is the Life Certificate requirement?\"\n\n"
        "Just type your question!"
    ),
    "off_topic": (
        "I am an AI assistant built exclusively for the Dayanand Social Security Scheme (DSSY), "
        "Department of Social Welfare, Government of Goa. "
        "I am not able to help with that query.\n\n"
        "However, here is what I can do for you:\n\n"
        "  Run SQL queries on the live DSSY beneficiary database and show results as tables\n"
        "  Visualize data as bar charts, donut charts, and line graphs\n"
        "  Answer scheme questions — eligibility, documents, pension amounts, amendments\n"
        "  Analyse trends — district-wise, taluka-wise, category-wise, payment compliance"
    ),
}


def detect_edge_case(question: str) -> dict | None:
    """
    Returns {type, response} if edge case detected.
    Returns None if legitimate SQL/RAG query.
    """
    q = question.strip()
    if len(q) < 2:
        return {"type": "confused", "response": _RESPONSES["confused"]}

    ql = q.lower()

    # Order matters — most specific first
    for pattern in _GREETINGS:
        if re.search(pattern, ql):
            return {"type": "greeting", "response": _RESPONSES["greeting"]}

    for pattern in _IDENTITY:
        if re.search(pattern, ql):
            return {"type": "identity", "response": _RESPONSES["identity"]}

    for pattern in _THANKS:
        if re.search(pattern, ql):
            return {"type": "thanks", "response": _RESPONSES["thanks"]}

    for pattern in _GOODBYE:
        if re.search(pattern, ql):
            return {"type": "goodbye", "response": _RESPONSES["goodbye"]}

    for pattern in _PROFANITY_REDIRECT:
        if re.search(pattern, ql):
            return {"type": "profanity", "response": _RESPONSES["profanity"]}

    for pattern in _SILLY:
        if re.search(pattern, ql):
            return {"type": "silly", "response": _RESPONSES["silly"]}

    for pattern in _CONFUSED:
        if re.search(pattern, ql):
            return {"type": "confused", "response": _RESPONSES["confused"]}

    # Whitelist: ONLY allow questions related to DSSY context
    # If none of these words are present — it's off-topic, block it
    dssy_words = [
        # Scheme names
        "dssy", "dsss", "dayanand", "social security",
        # Beneficiary terms
        "beneficiar", "pension", "scheme", "welfare",
        # Geography
        "taluka", "district", "goa", "bardez", "salcete", "tiswadi",
        "bicholim", "pernem", "sattari", "canacona", "quepem", "sanguem",
        "mormugao", "dharbandora", "ponda", "north goa", "south goa", "panaji", "margao",
        # Categories
        "senior", "widow", "widows", "disabled", "hiv", "aids", "single woman",
        "category", "categories",
        # Financial
        "payment", "payout", "monthly amount", "financial assistance",
        "rs\.", "rupee", "lakh", "crore",
        # Admin
        "eligible", "eligib", "registration", "active", "inactive", "deceased",
        "village", "life certificate", "aadhaar", "documents", "apply",
        "director", "social welfare", "amendment", "grievance",
        # Analytics
        "count", "total", "how many", "show", "list", "compare",
        "trend", "distribution", "breakdown", "compliance", "gender",
        "age", "statistic", "analytic", "data", "report",
    ]
    has_dssy_context = any(re.search(w, ql) for w in dssy_words)

    # If question has no DSSY context at all — block it
    if not has_dssy_context:
        return {"type": "off_topic", "response": _RESPONSES["off_topic"]}

    return None