package aggregation;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import state.SliceState;

import java.util.*;

/*
计算漏斗的聚合函数, 同时能够保存人群, 步骤一

目标: 查询12月1号到20号20天, 时间窗口为7天, 事件个数为3个的漏斗
select xwho, ld_count(xwhen, 7*86400000, xwhat, 'A,B,C') as xwho_state
from tablename
where ds >= '2016-12-01' and ds < '2016-12-21' and xwhat in ('A', 'B', 'C')
group by xwho;

输出结果:
0001 2
0002 1
0003 2
 */
@AggregationFunction("ld_count")
public class AggregationLDCount extends AggregationBase {

    private static final int COUNT_FLAG_LENGTH = 10;     // 状态变量最前3位存放临时变量(1, 1, 8)
    private static final int COUNT_ONE_LENGTH = 8;       // 状态变量中每个事件和其时间所占位数(long)

    @InputFunction
    public static void input(SliceState state,                                  // 每个用户的状态
                             @SqlType(StandardTypes.BIGINT) long xwhen,         // 当前事件的时间戳
                             @SqlType(StandardTypes.BIGINT) long windows,       // 当前查询的时间窗口大小
                             @SqlType(StandardTypes.VARCHAR) Slice xwhat,       // 当前事件的名称, A还是B或者C
                             @SqlType(StandardTypes.VARCHAR) Slice events) {    // 当前查询的全部事件, 逗号分隔
        // 获取状态
        Slice slice = state.getSlice();

        // 判断是否需要初始化events
        if (!event_pos_dict.containsKey(events)) {
            init_events(events, 0);
        }

        // 初始化slice
        if (null == slice) {
            slice = Slices.allocate(COUNT_FLAG_LENGTH);

            // 初始化前3位存放临时变量: {是否包含事件A(byte), 事件个数(byte), 时间窗口大小(long)}
            slice.setByte(0, 0);
            slice.setByte(1, event_pos_dict.get(events).size());
            slice.setLong(2, windows);

        }

        // 新建slice, 并初始化
        int slice_length = slice.length();
        Slice new_slice = Slices.allocate(slice_length + COUNT_ONE_LENGTH);
        new_slice.setBytes(0, slice.getBytes());

        // 更改状态变量
        byte xwhat_index = event_pos_dict.get(events).get(xwhat);
        if (xwhat_index == 0) {
            new_slice.setByte(0, 1);
        }
        new_slice.setLong(slice_length, xwhen * 10 + xwhat_index);

        // 返回结果
        state.setSlice(new_slice);
    }

    @CombineFunction
    public static void combine(SliceState state, SliceState otherState) {
        // 获取状态
        Slice slice = state.getSlice();
        Slice otherslice = otherState.getSlice();

        // 更新状态, 并返回结果
        if (null == slice) {
            state.setSlice(otherslice);
        } else {
            int length1 = slice.length();
            int length2 = otherslice.length();

            // 初始化
            Slice slice_new = Slices.allocate(length1 + length2 - COUNT_FLAG_LENGTH);

            // 赋值
            slice_new.setBytes(0, slice.getBytes());
            slice_new.setBytes(length1, otherslice.getBytes(), COUNT_FLAG_LENGTH, length2 - COUNT_FLAG_LENGTH);
            if (otherslice.getByte(0) == 1) {
                slice_new.setByte(0, 1);
            }

            // 返回结果
            state.setSlice(slice_new);
        }
    }

    @OutputFunction(StandardTypes.INTEGER)
    public static void output(SliceState state, BlockBuilder out) {
        // 获取状态
        Slice slice = state.getSlice();

        // 数据为空, 或者没有事件A, 返回0
        if ((null == slice) || (slice.getByte(0) == 0)) {
            out.writeInt(0);
            out.closeEntry();
            return;
        }

        // 构造列表和字典, 为排序做准备
        List<Long> time_array = new ArrayList<>();
        for (int index = COUNT_FLAG_LENGTH; index < slice.length(); index += COUNT_ONE_LENGTH) {
            time_array.add(slice.getLong(index));
        }

        // 排序数组, 这里可能比较耗时
        Collections.sort(time_array);

        // 获取中间变量
        byte events_count = slice.getByte(1);
        long windows = slice.getLong(2);

        // 遍历时间戳数据, 也就是遍历有序事件, 并构造结果
        int max_xwhat_index = 0;
        List<long[]> temp = new ArrayList<>();
        for (long xwhen_xwhat: time_array) {
            // 事件有序进入
            long timestamp = xwhen_xwhat / 10;
            byte xwhat = (byte) (xwhen_xwhat % 10);

            if (xwhat == 0) {
                // 新建临时对象, 存放 (A事件的时间戳, 当前最后一个事件的下标)
                long[] flag = {timestamp, xwhat};
                temp.add(flag);
            } else {
                // 更新临时对象: 从后往前, 并根据条件适当跳出
                for (int i = temp.size() - 1; i >= 0; --i) {
                    long[] flag = temp.get(i);
                    if ((timestamp - flag[0]) >= windows) {
                        // 当前事件的时间戳减去flag[0]超过时间窗口不合法, 跳出
                        break;
                    } else if (xwhat == (flag[1] + 1)) {
                        // 当前事件为下一个事件, 更新数据并跳出
                        flag[1] = xwhat;
                        if (max_xwhat_index < xwhat) {
                            max_xwhat_index = xwhat;
                        }
                        break;
                    }
                }

                // 漏斗流程结束, 提前退出
                if ((max_xwhat_index + 1) == events_count) {
                    break;
                }
            }
        }

        // 返回结果
        out.writeInt(max_xwhat_index + 1);
        out.closeEntry();
    }
}
