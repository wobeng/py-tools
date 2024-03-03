from py_tools.pylog import get_logger

logger = get_logger("py-tools.transcalculator")


class TranslationCostCalculator:
    def __init__(self):
        self.label = ""
        self.total_characters = 0
        self.cost_per_million_characters = 20.0

        
    def __call__(self, label):
        self.label = label
        return self

    def cost(self, characters):
        return characters * self.cost_per_million_characters / 1_000_000

    def add_characters(self, characters, label=""):
        """Add characters to the total count and calculate the cost for these characters."""

        cost = self.cost(len(characters))
        label = label or self.label
        self.total_characters += len(characters)
        total_cost = self.cost(self.total_characters)

        logger.info(
            f"{label} | Characters: {len(characters)}.  Total Characters: {self.total_characters}. Cost: {cost}. Total Cost: {total_cost}"
        )

    def get_total_cost(self):
        """Return the total characters translated and the total cost."""
        total_cost = self.cost(self.total_characters)
        logger.info(
            f"Session total characters: {self.total_characters}. Session total cost: {total_cost}"
        )
        # warn if total cost is above $1
        if total_cost > 1:
            logger.warning(
                f"Total cost for this session is over $1.00. Total cost: {total_cost}"
            )
        return self.total_characters, total_cost
