# P3
DBMS也可能做隐式排序，比如在执行DISTINCT和GROUP BY字句时，DBMS会隐式的对对应的列应用排序。

这个项目包含几个任务：

任务 #1：访问方法执行器（Access Method Executors）

任务 #2：聚合和连接执行器（Aggregation and Join Executors）

任务 #3：哈希连接执行器及优化（HashJoin Executor and Optimization）

任务 #4：排序 + 限制执行器 + 窗口函数 + Top-N 优化（Sort + Limit Executors + Window Functions + Top-N Optimization）

在公共 BusTub 仓库中，我们提供了一个完整的查询处理层。你可以使用 BusTub shell 来执行 SQL 查询，就像在其他数据库系统中一样。使用以下命令来编译并运行 BusTub shell
cd build && make -j$(nproc) shell -------- ./bin/bustub-shell

在收到一条 SQL 语句后，查询处理层首先会通过解析器 Parser 将 SQL 语句解析为一颗抽象句法树 AST（Abstracct Syntax Tree）。
接下来绑定器 Binder 会遍历这棵语法树，将表名、列名等映射到数据库中的实际对象上，并由计划器 Planner 生成初步的查询计划。查询计划会以树的形式表示，
数据从叶子节点流向父节点。最后，优化器 Optimizer 会优化生成最终的查询计划，然后交由查询执行层的执行器执行，而这里面的部分执行器需要我们来实现。

首先，Bustub 有一个 Catalog。Catalog 维护了几张 hashmap，保存了 table id 和 table name 到 table info 的映射关系。table id 由 Catalog 在新建 table 时自动分配，table name 则由用户指定。

这里的 table info 包含了一张 table 信息，有 schema、name、id 和指向 table heap 的指针。系统的其他部分想要访问一张 table 时，先使用 name 或 id 从 Catalog 得到 table info，再访问 table info 中的 table heap。

table heap 是管理 table 数据的结构，包含 table 相关操作。table heap 可能由多个 table page 组成，仅保存其第一个 table page 的 page id。需要访问某个 table page 时，通过 page id 经由 buffer pool 访问。

table page 是实际存储 table 数据的结构，当需要新增 tuple 时，table heap 会找到当前属于自己的最后一张 table page，尝试插入，若最后一张 table page 已满，则新建一张 table page 插入 tuple。table page 低地址存放 header，tuple 从高地址也就是 table page 尾部开始插入。

tuple 对应数据表中的一行数据。每个 tuple 都由 RID 唯一标识。RID 由 page id + slot num 构成。tuple 由 value 组成，value 的个数和类型由 table info 中的 schema 指定。value 则是某个字段具体的值，value 本身还保存了类型信息。

## Task1 - 访问方法执行器

### seq_scan_executor
SeqScanExecutor 遍历一个表，并逐个返回其元组
####  Init()
定义一些变量主要是方便后续的使用,

private:

/** The sequential scan plan node to be executed */

const SeqScanPlanNode *plan_;

TableInfo *table_info_;

TableHeap *table_heap_;  //表堆是表示物理表在磁盘上的存储结构

std::vector<RID> rids_;

std::vector<RID>::iterator rids_iter_;

首先获得tableinfo再获得tableheap，table_heap中有迭代去，获取迭代器，然后将table_heap中的所有页加入到rids_数组中。将迭代器指向第一个page;



#### Next(Tuple *tuple, RID *rid) -> bool

Next方法用于从表中检索下一个满足条件的元组。它使用一个do-while循环来遍历rids_ 列表中的 RID。对于每个 RID，它首先获取该 RID 对应的元组的元数据（TupleMeta）。如果该元组没有被删除（!meta.is_deleted_），则将该元组复制到传入的 tuple参数中，并将 RID 复制到rid参数中。
然后，它检查是否有过滤条件（plan_->filter_predicate_）。如果有，它会评估这个过滤条件，并检查结果是否为 true。只有当元组没有被删除且满足过滤条件时，do-while 循环才会结束。

### inser_executor-Next()
要求： 执行器生成一个整数类型的元组作为输出，并指示有多少行已经被插入到表中。当向表中插入数据时，如果表有关联的索引，请记得更新这些索引。

思路：把要插入的内容放在子执行器 child_executor_中，然后获取一些必要参数，其中插入到表中的行数的整数元组 和插入操作产生的下一个元组RID已经给出了。用while语句把子执行器中的每个元组都插进去（还有更新索引），最后算下一共插入多少行

获取table_info,和事务，通过child_executor_->Next(&child_tuple, rid)，获取tuple,和Rid然后调用table_heap->InsertTuple插入元组

获取待插入的表信息及其索引列表，从子执行器 child_executor_ 中逐个获取元组并插入到表中，同时更新所有的索引，next函数是虚函数，会自动调用seq_scan中的实现
插入元组std::optional<RID> new_rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, *tuple);
遍历所有索引，为每个索引更新对应的条目(从元组中提取索引键,向索引中插入键和新元组的RID);

`for (auto &index_info : indexes) {
// 从元组中提取索引键
auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
// 向索引中插入键和新元组的RID
index_info->index_->InsertEntry(key, new_rid, this->exec_ctx_->GetTransaction());
}`
,创建了一个 vector对象values，其中包含了一个 Value 对象。这个 Value 对象表示一个整数值，值为 count,这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数

std::vector<Value> result = {{TypeId::INTEGER, count}};

*tuple = Tuple(result, &GetOutputSchema());

### update_executor-Next

要求：执行器将输出一个整数类型的元组，表示已更新的行数。

思路： 更新逻辑是把原来的元组设置为已删除，然后插入新的元组。

获取table_info indexes相关基本信息，后面需要使用->更新逻辑不是直接更新相应的值，而是把原来的元组设置为已删除，然后插入新的元组->
将每个元组都标记为已删除;table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);->获取要插入的新元组

for (const auto &expr : plan_->target_expressions_) {

new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));

}-> 插入新的元組std::optional<RID> new_rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, update_tuple);->
遍历所有索引，为每个索引更新对应的条目 RID new_rid = new_rid_optional.value();
// 更新所有相关索引，需要先删除直接的索引，然后插入新的索引信息
for (auto &index_info : indexes) {

auto index = index_info->index_.get();

auto key_attrs = index_info->index_->GetKeyAttrs();

auto old_key = child_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);

auto new_key = update_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);

// 从索引中删除旧元组的条目

index->DeleteEntry(old_key, child_rid, this->exec_ctx_->GetTransaction());

// 向索引中插入新元组的条目

index->InsertEntry(new_key , new_rid, this->exec_ctx_->GetTransaction());
}


Delete 和update类似

###  Indexscan-索引扫描-Init

要求:使用哈希索引执行点查找以检索元组的RID。然后逐个发出这些元组。

思路:把索引得到的值放到result_rids里，然后判断result_rids 是不是空，删除标志，都不是就把数据和RID分别赋值给传入的 tuple 和 rid 指针所指向的变量

获取索引( IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);, auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());)
->清空存储记录标识符 (RIDs) 的向量，以确保没有残留数据。-> 如果 `plan_` 中存在过滤谓词（`filter_predicate_`），则进行索引扫描。->将过滤谓词的右子表达式（通常是常量值）转换为 `ConstantValueExpression` 类型的指针。
const auto right_exptr = dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());-> 获取常量值 `v`，即谓词中要查找的值。
Value v = right_exptr->val_;->使用哈希索引的 `ScanKey` 函数查找所有符合条件的记录标识符 (RIDs)。通过构造一个带有单个值 `v` 的元组以及索引的键模式来进行查找，并将结果存入 `rids_` 向量中。
hash_index->ScanKey(Tuple{{v}, index_info->index_->GetKeySchema()}, &rids_, GetExecutorContext()->GetTransaction());
###  Indexscan-索引扫描-Next
从table_info 获取元素 table_info->table_->GetTupleMeta(*rid)，通过table_info获取tuple,meta.is_deleted_为false就终止


### 优化
要求： 实现将SeqScanPlanNode优化为IndexScanPlanNode的优化器规则
思路：
只有一种情况可以把顺序的变成索引的，即以下条件同时满足
1、谓词（即where之后的语句）不为空
2、表支持索引扫描
3、只有一个谓词条件（如SELECT * FROM t1 WHERE v1 = 1 AND v2 = 2就不行）
4、谓词是等值条件（即WHERE v1 = 1）
所以要把所有条件都判断一遍，才能用索引扫描


## Task2 - Aggregation & Join Executors

Aggregation
1.1 思路
注意：聚合在查询计划中是管道中断者，聚合操作不能简单地按行（tuple-by-tuple）从其子节点（或数据源）接收数据，然后逐行输出结果。
相反，聚合操作需要先从其子节点获取所有相关的数据，完成整个聚合计算过程（例如计算总和、平均值、最小/最大值等），然后才能产生输出结果
AggregationExecutor 需要先从子执行器中获取所有数据，然后对这些数据进行分组和聚合操作，
最后将结果输出，而这个过程必须在init函数中完成

我们以分组聚合查询语句 select count(name) from table group by camp; 为例简要说明一下聚合执行器的执行流程：
Init() 函数首先从子执行器中逐行获取数据，并根据每行数据构建聚合键和聚合值。其中聚合键用于标识该行数据属于哪一个聚合组，这里是按照阵营 camp 分组，因此聚合键会有 Piltover、Ionia 和 Shadow Isles 三种取值，这样所有数据被分成三个聚合组。而聚合值就是待聚合的列的值，这里的聚合列是 name，因此这五个 Tuple 中生成的聚合值即为对应的 name 属性的值。
对于每个提取的数据行，Init() 函数还会通过 InsertCombine()， 将相应的聚合值聚合到到相应的聚合组中。在InsertCombine()中调用CombineAggregateValues() 函数来实现具体的聚合规则。
经过 Init() 函数的处理，以上六条数据会被整理为 [{“Piltover”: 3}, {“Ionia”: 2}, {“Shadow Isles”: 1}] 三个聚合组（对应于聚合哈希表中的三个键值对）。其中groupby的值分别为Piltover、Ionia、Shadow Isles；aggregate的值分别为3、2、1。
最后，Next() 函数会通过哈希迭代器依次获取每个聚合组的键与值，返回给父执行器。如果没 group by 子句，那么所有数据都会被分到同一个聚合组中并返回。

### void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) 

将输入合并到聚合结果中。

依照不同的聚合操作类型（agg_types_）进行不同的操作->遍历聚合表达式，获取聚合值和要操作的聚合值，agg_types_[i]获取第i+1个表达式的类型，
然后分类计算。


      Value &old_val = result->aggregates_[i];
      const Value &new_val = input.aggregates_[i];
      switch (agg_types_[i]) {
          //无论Value是否为null，均统计其数目
        case AggregationType::CountStarAggregate:
          old_val = old_val.Add(Value(TypeId::INTEGER, 1));
          break;
          //统计非null值
        case AggregationType::CountAggregate:
          if (!new_val.IsNull()) {
            if (old_val.IsNull()) {
              old_val = ValueFactory::GetIntegerValue(0);
            }
            old_val = old_val.Add(Value(TypeId::INTEGER, 1));
          }
          break;
        case AggregationType::SumAggregate:
          if (!new_val.IsNull()) {
            if (old_val.IsNull()) {
              old_val = new_val;
            } else {
              old_val = old_val.Add(new_val);
            }
          }
          break;
        case AggregationType::MinAggregate:

          if (!new_val.IsNull()) {
            if (old_val.IsNull()) {
              old_val = new_val;
            } else {
              old_val = new_val.CompareLessThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
            }
          }
          break;
        case AggregationType::MaxAggregate:
          if (!new_val.IsNull()) {
            if (old_val.IsNull()) {
              old_val = new_val;
            } else {
              old_val = new_val.CompareGreaterThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
            }
          }
          break;


### void Init()-初始化聚合

获取聚合表达式->获取聚合类型->根据聚合表达式以及聚合类型创建哈希表,SimpleAggregationHashTable是一个在聚合查询中专为计算聚合设计的散列表（哈希表
,它用于快速地分组数据，aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());并对每个分组应用聚合函数。->遍历子执行器，将子执行器中的获取的数据插入到聚合哈希表中->不能在聚合执行器中完成，因为聚合执行器
需要先从子执行器中获取所有数据，然后对这些数据进行分组和聚合操作，最后才能产生输出结果. while (child_executor_->Next(&child_tuple, &rid))
通过tuple获取聚合键和聚合值, 聚合键在聚合操作中用来区分不同的分组auto agg_key = MakeAggregateKey(&child_tuple);auto agg_val = MakeAggregateValue(&child_tuple);
将聚合键和聚合值插入到聚合哈希表中aht_->InsertCombine(agg_key, agg_val);

### Next
获取聚合键和聚合值->根据聚合键和聚合值生成查询结果元组->遍历聚合键和聚合值，生成查询结果元组,根据文件要求，有groupby和aggregate两个部分的情况下
，groupby也要算上，都添加到value中

values.reserve(agg_key.group_bys_.size() + agg_val.aggregates_.size());

for (auto &group_values : agg_key.group_bys_) {

values.emplace_back(group_values);

}
for (auto &agg_value : agg_val.aggregates_) {

values.emplace_back(agg_value);

}
*tuple = {values, &GetOutputSchema()};

//迭代到下一个聚合键和聚合值

++*aht_iterator_;

// 表示成功返回了一个聚合结果

return true;

若没有groupby语句则生成一个初始的聚合值元组并返回

## NestedLoopJoin

思路

实现内连接和左外连接

左外连接：左边确定找右边，分两种情况：1、找得到：把两个一样的放一块。2、找不到：左边写好右边放null。

内连接：也是左边确定找右边，找到就放value里，找不到就不放。

### LeftAntiJoinTuple(Tuple *left_tuple) -> Tuple

左外连接初始化：先找左边的放入values数组并将右边不存在的置为空

内连接的初始化，就是遍历左右两边，分别加入value

### Next
进入循环，直到返回结果或者遍历结束,如果左表中的元组已经遍历完毕 返回 false，表示没有更多的结果了,
如果右表中的元组已经遍历完毕且如果是左连接且左表中的当前元组还没有匹配到右表的任何元组
生成左连接结果中不存在右表元组的元组，获取元组的 RID，标记左表中的当前元组已经处理过，返回 true，表示找到了一个结果。若不是左连接，


    if (!right_executor_->Next(&right_tuple, &right_rid)) {  // 如果右表中的元组已经遍历完毕
      if (plan_->GetJoinType() == JoinType::LEFT &&
          !left_done_) {  // 如果是左连接且左表中的当前元组还没有匹配到右表的任何元组
        *tuple = LeftAntiJoinTuple(&left_tuple_);  // 生成左连接结果中不存在右表元组的元组
        *rid = tuple->GetRid();                    // 获取元组的 RID

        left_done_ = true;  // 标记左表中的当前元组已经处理过
        return true;        // 返回 true，表示找到了一个结果
      }

      right_executor_->Init();  // 重新初始化右表的扫描，以便重新开始扫描右表
      left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);  // 获取下一个左表中的元组
      left_done_ = false;                                         // 重置 left_done_ 标志
      continue;                                                   // 继续循环，尝试下一个左表中的元组
    }

若右表没有遍历完毕

    auto ret = plan_->Predicate()->EvaluateJoin(
        &left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
        right_executor_->GetOutputSchema());   // 利用 JOIN 条件判断左右表中的元组是否满足条件
    if (!ret.IsNull() && ret.GetAs<bool>()) {  // 如果左右表中的元组满足 JOIN 条件
      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);  // 生成内连接结果的元组
      *rid = tuple->GetRid();                               // 获取元组的 RID

      left_done_ = true;  // 标记左表中的当前元组已经处理过
      return true;        // 返回 true，表示找到了一个结果
    }

## Task 3-HashJoin 执行器与优化

1、HashJoin
1.1 思路
哈希连接包括两个阶段：构建（build）阶段和探测（probe）阶段。

构建阶段：遍历右表，将每个元组的连接键哈希并存储在哈希表中。
探测阶段：遍历左表，对表中的每个元组进行哈希，并在哈希表中查找具有相同哈希值的条目。由于右表可能有好几个和左表匹配的选项，所以还需要一个迭代器

其中需要注意，如果是左连接，没找到对应哈希值要把左边对应的右边写null。如果是内连接，跳过下一个。

### Init()

// 初始化左右plan的左右孩子
this->left_child_->Init();
this->right_child_->Init();

// 获取左执行器符合条件的元组，left_bool_用于判断左执行器是否还有符合条件的元组
left_bool_ = left_child_->Next(&left_tuple_, &left_rid_);

// NEXT方法的輸出參數，用于存储查询结果
Tuple right_tuple{};
RID right_rid{};

//构建哈希表
jht_ = std::make_unique<SimpleHashJoinHashTable>();

// 遍历子执行器，将右子执行器中的获取的数据插入到join哈希表中
// 不能在HashJoinExecutor执行器的next中完成，因为执行器需要先从子执行器中获取所有数据，然后对这些数据进行join，最后才能产生输出结果

while (right_child_->Next(&right_tuple, &right_rid)) {

jht_->InsertKey(GetRightJoinKey(&right_tuple), right_tuple);

}

// 获取左侧元组的hash key
auto left_hash_key = GetLeftJoinKey(&left_tuple_);
// 在哈希表中查找与左侧元组匹配的右侧元组
right_tuple_ = jht_->GetValue(left_hash_key);

//这里必须判断right_tuple_是否为空，否则指针会指向空地址报错

// 不为空说明找到了哈希值一样的

if (right_tuple_ != nullptr) {

jht_iterator_ = right_tuple_->begin();

// 标记为true，防止next函数中重复输出

has_done_ = true;

} else {

// 标记为false，主要用于左连接没有匹配的情况

has_done_ = false;

}

### Next(Tuple *tuple, RID *rid) -> bool
用while的原因：如果是内连接，如果没有匹配的元组，则该轮不输出任何元组，不需要返回值，继续往下查找其他左元组while (true)

// 如果right_tuple_不为空，且jht_iterator_（right_tuple_的指针）未遍历完，则遍历输出
// 一个左边可能匹配多个右边

      std::vector<Value> values;
      auto right_tuple = *jht_iterator_;
      for (uint32_t i = 0; i < this->left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null
      for (uint32_t i = 0; i < this->right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&this->right_child_->GetOutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      ++jht_iterator_;
      return true;



//如果right_tuple_为空，或者jht_iterator_遍历完，且为左连接
// 如果has_done_为false，则说明左连接没有匹配的元组，需要输出右元组为null的情况


    if (plan_->GetJoinType() == JoinType::LEFT && !has_done_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < this->left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null
      for (uint32_t i = 0; i < this->right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(this->right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      has_done_ = true;
      return true;
    }
    // 如果不是左连接，或者为左连接，但有有效输出，则继续遍历下一个左元组进行匹配
    // 如果left_bool_为false，左边找完了
    left_bool_ = left_child_->Next(&this->left_tuple_, &this->left_rid_);
    if (!left_bool_) {
      return false;
    }
    // 重置右边匹配的元组，以及更新迭代器
    auto left_hash_key = GetLeftJoinKey(&left_tuple_);
    // 在哈希表中查找与左侧元组匹配的右侧元组
    right_tuple_ = jht_->GetValue(left_hash_key);
    if (right_tuple_ != nullptr) {
      jht_iterator_ = right_tuple_->begin();
      has_done_ = true;
    } else {
      has_done_ = false;
    }


### NestedLoopJoin优化为HashJoin

2.1 思路
查询计划是从下往上的树形结构，所以要现在做下面再搞上面（用递归实现）
注意：要检查每个等值条件两侧的列属于哪个表。
步骤：
1、把子节点用递归的方式添加到 optimized_children 列表中
2、用 CloneWithChildren 方法克隆原始计划，并用优化后的子节点替换原始的子节点。这样即使实际没优化成，也说明尝试优化过了
3、看优化为hashjoin的条件满不满足
4、满足则换，不满足输出原计划

解析一个逻辑表达式，并提取出左右两侧的关键表达式，尝试将谓词转换为逻辑表达式，与或非

#### auto *logic_expression_ptr = dynamic_cast<LogicExpression *>(predicate.get());

递归处理逻辑逻辑表达式

if (logic_expression_ptr != nullptr) {
// left child
ParseAndExpression(logic_expression_ptr->GetChildAt(0), left_key_expressions, right_key_expressions);
// right child
ParseAndExpression(logic_expression_ptr->GetChildAt(1), left_key_expressions, right_key_expressions);
}

尝试将谓词转换为比较表达式

auto *comparison_ptr = dynamic_cast<ComparisonExpression *>(predicate.get());
// 如果是比较表达式

     if (comparison_ptr != nullptr) {
    auto column_value_1 = dynamic_cast<const ColumnValueExpression &>(*comparison_ptr->GetChildAt(0));
    // auto column_value_2 = dynamic_cast<const ColumnValueExpression &>(*comparison_ptr->GetChildAt(1));
    // 区分每个数据元素是从左侧表还是右侧表提取的，例如 A.id = B.id时，系统需要知道 A.id 和 B.id 分别属于哪个数据源
    if (column_value_1.GetTupleIdx() == 0) {
      left_key_expressions->emplace_back(comparison_ptr->GetChildAt(0));
      right_key_expressions->emplace_back(comparison_ptr->GetChildAt(1));
    } else {
      left_key_expressions->emplace_back(comparison_ptr->GetChildAt(1));
      right_key_expressions->emplace_back(comparison_ptr->GetChildAt(0));
    } }

#### auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef 

       std::vector<AbstractPlanNodeRef> optimized_children;
       for (const auto &child : plan->GetChildren()) {
       // 递归调用
       optimized_children.emplace_back(OptimizeNLJAsHashJoin(child));
       }          
     auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));
     if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
       const auto &join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
       // 获取谓词
       auto predicate = join_plan.Predicate();
       std::vector<AbstractExpressionRef> left_key_expressions;
       std::vector<AbstractExpressionRef> right_key_expressions;
      // 提取左右两侧关键表达式，分别放到left_key_expressions和right_key_expressions里)
      ParseAndExpression(predicate, &left_key_expressions, &right_key_expressions);
      return std::make_shared<HashJoinPlanNode>(join_plan.output_schema_, join_plan.GetLeftPlan(),
      join_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
      join_plan.GetJoinType());
      }
      return optimized_plan;


## Task 4 Sort + Limit Executors + Top-N Optimization
### Sort
自定义比较器


    for (auto const &order_by : this->order_bys_) {
      const auto order_type = order_by.first;
      // 使用Evaluate获取值
      AbstractExpressionRef expr = order_by.second;
      Value v1 = expr->Evaluate(&t1, *schema_);
      Value v2 = expr->Evaluate(&t2, *schema_);
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }
      // 如果是升序（ASC 或 DEFAULT），比较 v1 是否小于 v2（CompareLessThan）
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      // 如果是降序（DESC），比较 v1 是否大于 v2（CompareGreaterThan）
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }
    // 两个元组所有键都相等
    return false;

Init函数排序好，next直接去元组

std::sort(tuples_.begin(), tuples_.end(), Comparator(&this->GetOutputSchema(), order_by));

### limit


      // 获取符合条件数量的元组
     while (count < limit && child_executor_->Next(&tuple, &rid)) {
       count++;
       tuples_.emplace_back(tuple);
     }

### top_N

自定义比较器



    auto operator()(const Tuple &t1, const Tuple &t2) -> bool {
    for (auto const &order_by : this->order_bys_) {
      const auto order_type = order_by.first;
      // 使用Evaluate获取值
      AbstractExpressionRef expr = order_by.second;
      Value v1 = expr->Evaluate(&t1, *schema_);
      Value v2 = expr->Evaluate(&t2, *schema_);
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }
      // 如果是升序（ASC 或 DEFAULT），比较 v1 是否小于 v2（CompareLessThan）
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      // 如果是降序（DESC），比较 v1 是否大于 v2（CompareGreaterThan）
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }
    // 两个元组所有键都相等
    return false;
    }

Init初始化好，Next直接获取元组

Init函数

    child_executor_->Init();
    //使用优先队列存储topN，升序用大顶堆，降序用小顶堆
    std::priority_queue<Tuple, std::vector<Tuple>, HeapComparator> heap(
    HeapComparator(&this->GetOutputSchema(), plan_->GetOrderBy()));
    Tuple tuple{};
     RID rid{};
    //遍历子执行器，将子执行器返回的元组加入优先队列
    while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    heap_size_++;
    //因為只需要topN个元组，所以当优先队列大小大于topN时，弹出堆顶元组（如果是升序，堆顶是最大的元组，如果是降序，堆顶是最小的元组）
    if (heap.size() > plan_->GetN()) {
    heap.pop();
    heap_size_--;
     }
    }
    while (!heap.empty()) {
    this->top_entries_.push(heap.top());
    heap.pop();
    }


    auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
      if (top_entries_.empty()) {
       return false;
       }
     *tuple = top_entries_.top();
     top_entries_.pop();
     return true;
     }